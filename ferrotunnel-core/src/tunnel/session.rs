use crate::rate_limit::{RateLimiterConfig, SessionRateLimiter};
use crate::stream::AnyMultiplexer;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Default number of shards for [`ShardedSessionStore`]. Tune for contention vs memory.
const DEFAULT_SESSION_STORE_SHARDS: usize = 16;

/// One shard: tunnel_id -> Uuid, and Uuid -> Session.
type SessionShard = (DashMap<String, Uuid>, DashMap<Uuid, Session>);

/// Represents an active client session
#[derive(Debug)]
pub struct Session {
    pub id: Uuid,
    pub tunnel_id: String,
    pub client_addr: SocketAddr,
    pub token: String,
    pub connected_at: Instant,
    pub last_heartbeat: Instant,
    pub capabilities: Vec<String>,
    pub multiplexer: Option<AnyMultiplexer>,
    pub rate_limiter: Option<SessionRateLimiter>,
}

impl Session {
    pub fn new(
        id: Uuid,
        tunnel_id: String,
        client_addr: SocketAddr,
        token: String,
        capabilities: Vec<String>,
        multiplexer: Option<AnyMultiplexer>,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            tunnel_id,
            client_addr,
            token,
            connected_at: now,
            last_heartbeat: now,
            capabilities,
            multiplexer,
            // Enforce per-session limits by default so the limiter is never dead code.
            // Callers may override via [`Self::with_rate_limiter`].
            rate_limiter: Some(SessionRateLimiter::new(&RateLimiterConfig::default())),
        }
    }

    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: SessionRateLimiter) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }
}

/// Error type for session store operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum SessionStoreError {
    #[error("tunnel ID '{0}' is already registered by another session")]
    TunnelIdAlreadyExists(String),
}

impl From<SessionStoreError> for ferrotunnel_common::TunnelError {
    fn from(err: SessionStoreError) -> Self {
        ferrotunnel_common::TunnelError::InvalidState(err.to_string())
    }
}

/// Thread-safe session store
#[derive(Debug, Clone, Default)]
pub struct SessionStore {
    sessions: Arc<DashMap<Uuid, Session>>,
    tunnel_index: Arc<DashMap<String, Uuid>>,
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            tunnel_index: Arc::new(DashMap::new()),
        }
    }

    /// Add a new session.
    /// Returns error if `tunnel_id` is already registered by a different session.
    pub fn add(&self, session: Session) -> Result<(), SessionStoreError> {
        let session_id = session.id;

        // Atomically claim the tunnel_id via the entry API to avoid a TOCTOU race
        // between the existence check and insertion (issue #118).
        match self.tunnel_index.entry(session.tunnel_id.clone()) {
            Entry::Occupied(entry) => {
                if *entry.get() != session_id {
                    return Err(SessionStoreError::TunnelIdAlreadyExists(
                        entry.key().clone(),
                    ));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(session_id);
            }
        }
        self.sessions.insert(session_id, session);
        Ok(())
    }

    /// Add or replace a session, removing any existing session with the same `tunnel_id`.
    /// Use this for explicit session replacement (e.g., reconnection).
    pub fn add_or_replace(&self, session: Session) {
        let tunnel_id = session.tunnel_id.clone();
        let session_id = session.id;

        // Remove existing session with the same tunnel_id if it exists
        if let Some((_, old_session_id)) = self.tunnel_index.remove(&tunnel_id) {
            if old_session_id != session_id {
                self.sessions.remove(&old_session_id);
            }
        }

        self.tunnel_index.insert(tunnel_id, session_id);
        self.sessions.insert(session_id, session);
    }

    /// Get a session by ID
    pub fn get(&self, id: &Uuid) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        self.sessions.get(id)
    }

    /// Get a session by `tunnel_id`
    pub fn get_by_tunnel_id(
        &self,
        tunnel_id: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        let id = self.tunnel_index.get(tunnel_id)?;
        self.sessions.get(&id)
    }

    /// Get a mutable session by ID (e.g. to update heartbeat)
    pub fn get_mut(&self, id: &Uuid) -> Option<dashmap::mapref::one::RefMut<'_, Uuid, Session>> {
        self.sessions.get_mut(id)
    }

    /// Remove a session
    pub fn remove(&self, id: &Uuid) -> Option<Session> {
        if let Some((_, session)) = self.sessions.remove(id) {
            self.tunnel_index.remove(&session.tunnel_id);
            Some(session)
        } else {
            None
        }
    }

    /// Count active sessions
    pub fn count(&self) -> usize {
        self.sessions.len()
    }

    /// Clean up stale sessions that haven't sent a heartbeat within the timeout
    /// Returns the number of removed sessions
    pub fn cleanup_stale_sessions(&self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        // Identify stale sessions
        // We use an explicit scope or just collect keys to avoid deadlocks strictly speaking,
        // though dashmap handles iteration safely.
        for r in self.sessions.iter() {
            if now.duration_since(r.last_heartbeat) > timeout {
                to_remove.push(*r.key());
            }
        }

        let count = to_remove.len();
        for id in to_remove {
            self.remove(&id);
        }

        count
    }

    pub fn find_multiplexer(&self) -> Option<AnyMultiplexer> {
        for r in self.sessions.iter() {
            if let Some(m) = &r.multiplexer {
                return Some(m.clone());
            }
        }
        None
    }

    /// Find a multiplexer for a session that has the specified capability
    pub fn find_multiplexer_with_capability(&self, capability: &str) -> Option<AnyMultiplexer> {
        for r in self.sessions.iter() {
            if r.capabilities.contains(&capability.to_string()) {
                if let Some(m) = &r.multiplexer {
                    return Some(m.clone());
                }
            }
        }
        None
    }
}

/// Sharded session store to reduce contention on tunnel_id lookups.
/// Same API as [`SessionStore`]; use when many concurrent lookups by tunnel_id are expected.
#[derive(Debug, Clone)]
pub struct ShardedSessionStore {
    shards: Arc<Vec<SessionShard>>,
    n_shards: usize,
}

fn shard_index(tunnel_id: &str, n_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    tunnel_id.hash(&mut hasher);
    (hasher.finish() as usize) % n_shards
}

impl ShardedSessionStore {
    /// Create a sharded store with the default number of shards (16).
    pub fn new() -> Self {
        Self::with_shards(DEFAULT_SESSION_STORE_SHARDS)
    }

    /// Create a sharded store with the given number of shards.
    pub fn with_shards(n_shards: usize) -> Self {
        let shards: Vec<SessionShard> = (0..n_shards)
            .map(|_| (DashMap::new(), DashMap::new()))
            .collect();
        Self {
            shards: Arc::new(shards),
            n_shards,
        }
    }

    /// Add a new session. Returns error if `tunnel_id` is already registered by a different session.
    pub fn add(&self, session: Session) -> Result<(), SessionStoreError> {
        let session_id = session.id;
        let idx = shard_index(&session.tunnel_id, self.n_shards);
        let (tunnel_index, sessions) = &self.shards[idx];

        // Atomically claim the tunnel_id on the selected shard via the entry API
        // to avoid a TOCTOU race between the existence check and insertion (issue #118).
        match tunnel_index.entry(session.tunnel_id.clone()) {
            Entry::Occupied(entry) => {
                if *entry.get() != session_id {
                    return Err(SessionStoreError::TunnelIdAlreadyExists(
                        entry.key().clone(),
                    ));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(session_id);
            }
        }
        sessions.insert(session_id, session);
        Ok(())
    }

    /// Add or replace a session, removing any existing session with the same `tunnel_id`.
    pub fn add_or_replace(&self, session: Session) {
        let tunnel_id = session.tunnel_id.clone();
        let session_id = session.id;
        let idx = shard_index(&tunnel_id, self.n_shards);
        let (tunnel_index, sessions) = &self.shards[idx];

        if let Some((_, old_id)) = tunnel_index.remove(&tunnel_id) {
            if old_id != session_id {
                sessions.remove(&old_id);
            }
        }
        tunnel_index.insert(tunnel_id, session_id);
        sessions.insert(session_id, session);
    }

    /// Get a session by ID. Requires scanning shards; prefer [`Self::get_by_tunnel_id`] when possible.
    pub fn get(&self, id: &Uuid) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        for (_, sessions) in &*self.shards {
            if let Some(r) = sessions.get(id) {
                return Some(r);
            }
        }
        None
    }

    /// Get a session by `tunnel_id` (shard-local, low contention).
    pub fn get_by_tunnel_id(
        &self,
        tunnel_id: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        let idx = shard_index(tunnel_id, self.n_shards);
        let (tunnel_index, sessions) = &self.shards[idx];
        let id = tunnel_index.get(tunnel_id)?;
        sessions.get(&id)
    }

    /// Get a mutable session by ID.
    pub fn get_mut(&self, id: &Uuid) -> Option<dashmap::mapref::one::RefMut<'_, Uuid, Session>> {
        for (_, sessions) in &*self.shards {
            if let Some(r) = sessions.get_mut(id) {
                return Some(r);
            }
        }
        None
    }

    /// Remove a session by ID.
    pub fn remove(&self, id: &Uuid) -> Option<Session> {
        for (tunnel_index, sessions) in &*self.shards {
            if let Some((_, session)) = sessions.remove(id) {
                tunnel_index.remove(&session.tunnel_id);
                return Some(session);
            }
        }
        None
    }

    /// Count active sessions (sum across shards).
    pub fn count(&self) -> usize {
        self.shards.iter().map(|(_, s)| s.len()).sum()
    }

    /// Clean up stale sessions. Returns the number of removed sessions.
    pub fn cleanup_stale_sessions(&self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut to_remove = Vec::new();
        for (_, sessions) in &*self.shards {
            for r in sessions {
                if now.duration_since(r.last_heartbeat) > timeout {
                    to_remove.push(*r.key());
                }
            }
        }
        let count = to_remove.len();
        for id in to_remove {
            self.remove(&id);
        }
        count
    }

    /// Find any multiplexer (scans shards).
    pub fn find_multiplexer(&self) -> Option<AnyMultiplexer> {
        for (_, sessions) in &*self.shards {
            for r in sessions {
                if let Some(m) = &r.multiplexer {
                    return Some(m.clone());
                }
            }
        }
        None
    }

    /// Find a multiplexer for a session that has the specified capability.
    pub fn find_multiplexer_with_capability(&self, capability: &str) -> Option<AnyMultiplexer> {
        for (_, sessions) in &*self.shards {
            for r in sessions {
                if r.capabilities.contains(&capability.to_string()) {
                    if let Some(m) = &r.multiplexer {
                        return Some(m.clone());
                    }
                }
            }
        }
        None
    }
}

impl Default for ShardedSessionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Session store backend: default (single DashMap pair) or sharded (for high contention).
#[derive(Clone)]
pub enum SessionStoreBackend {
    Default(SessionStore),
    Sharded(ShardedSessionStore),
}

impl SessionStoreBackend {
    pub fn add(&self, session: Session) -> Result<(), SessionStoreError> {
        match self {
            SessionStoreBackend::Default(s) => s.add(session),
            SessionStoreBackend::Sharded(s) => s.add(session),
        }
    }
    pub fn add_or_replace(&self, session: Session) {
        match self {
            SessionStoreBackend::Default(s) => s.add_or_replace(session),
            SessionStoreBackend::Sharded(s) => s.add_or_replace(session),
        }
    }
    pub fn get(&self, id: &Uuid) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        match self {
            SessionStoreBackend::Default(s) => s.get(id),
            SessionStoreBackend::Sharded(s) => s.get(id),
        }
    }
    pub fn get_by_tunnel_id(
        &self,
        tunnel_id: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        match self {
            SessionStoreBackend::Default(s) => s.get_by_tunnel_id(tunnel_id),
            SessionStoreBackend::Sharded(s) => s.get_by_tunnel_id(tunnel_id),
        }
    }
    pub fn get_mut(&self, id: &Uuid) -> Option<dashmap::mapref::one::RefMut<'_, Uuid, Session>> {
        match self {
            SessionStoreBackend::Default(s) => s.get_mut(id),
            SessionStoreBackend::Sharded(s) => s.get_mut(id),
        }
    }
    pub fn remove(&self, id: &Uuid) -> Option<Session> {
        match self {
            SessionStoreBackend::Default(s) => s.remove(id),
            SessionStoreBackend::Sharded(s) => s.remove(id),
        }
    }
    pub fn count(&self) -> usize {
        match self {
            SessionStoreBackend::Default(s) => s.count(),
            SessionStoreBackend::Sharded(s) => s.count(),
        }
    }
    pub fn cleanup_stale_sessions(&self, timeout: Duration) -> usize {
        match self {
            SessionStoreBackend::Default(s) => s.cleanup_stale_sessions(timeout),
            SessionStoreBackend::Sharded(s) => s.cleanup_stale_sessions(timeout),
        }
    }
    pub fn find_multiplexer_with_capability(&self, capability: &str) -> Option<AnyMultiplexer> {
        match self {
            SessionStoreBackend::Default(s) => s.find_multiplexer_with_capability(capability),
            SessionStoreBackend::Sharded(s) => s.find_multiplexer_with_capability(capability),
        }
    }
}

impl Default for SessionStoreBackend {
    fn default() -> Self {
        SessionStoreBackend::Default(SessionStore::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_session_management() {
        let store = SessionStore::new();
        let id = Uuid::new_v4();
        let addr = "127.0.0.1:1234".parse().unwrap();
        let session = Session::new(id, "test-tunnel".into(), addr, "token".into(), vec![], None);

        store.add(session).unwrap();
        assert_eq!(store.count(), 1);

        assert!(store.get(&id).is_some());
        assert!(store.get_by_tunnel_id("test-tunnel").is_some());

        let removed = store.remove(&id);
        assert!(removed.is_some());
        assert_eq!(store.count(), 0);
        assert!(store.get_by_tunnel_id("test-tunnel").is_none());
    }

    #[test]
    fn test_stale_cleanup() {
        let store = SessionStore::new();
        let id = Uuid::new_v4();
        let addr = "127.0.0.1:1234".parse().unwrap();
        let mut session =
            Session::new(id, "test-tunnel".into(), addr, "token".into(), vec![], None);

        // Artificially age the session
        session.last_heartbeat = Instant::now()
            .checked_sub(Duration::from_secs(100))
            .unwrap();
        store.add(session).unwrap();

        let count = store.cleanup_stale_sessions(Duration::from_secs(90));
        assert_eq!(count, 1);
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn test_tunnel_id_uniqueness() {
        let store = SessionStore::new();
        let addr = "127.0.0.1:1234".parse().unwrap();

        // Add first session
        let id1 = Uuid::new_v4();
        let session1 = Session::new(id1, "my-tunnel".into(), addr, "token1".into(), vec![], None);
        assert!(store.add(session1).is_ok());

        // Try to add another session with the same tunnel_id - should fail
        let id2 = Uuid::new_v4();
        let session2 = Session::new(id2, "my-tunnel".into(), addr, "token2".into(), vec![], None);
        let result = store.add(session2);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SessionStoreError::TunnelIdAlreadyExists(_)
        ));

        // Only one session should exist
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn test_add_or_replace() {
        let store = SessionStore::new();
        let addr = "127.0.0.1:1234".parse().unwrap();

        // Add first session
        let id1 = Uuid::new_v4();
        let session1 = Session::new(id1, "my-tunnel".into(), addr, "token1".into(), vec![], None);
        store.add_or_replace(session1);
        assert_eq!(store.count(), 1);

        // Replace with new session with same tunnel_id
        let id2 = Uuid::new_v4();
        let session2 = Session::new(id2, "my-tunnel".into(), addr, "token2".into(), vec![], None);
        store.add_or_replace(session2);

        // Should still have only one session
        assert_eq!(store.count(), 1);

        // The old session should be gone, new one should exist
        assert!(store.get(&id1).is_none());
        assert!(store.get(&id2).is_some());
        assert!(store.get_by_tunnel_id("my-tunnel").is_some());
    }

    #[test]
    fn test_sharded_store_same_api() {
        let store = ShardedSessionStore::with_shards(4);
        let id = Uuid::new_v4();
        let addr = "127.0.0.1:1234".parse().unwrap();
        let session = Session::new(
            id,
            "shard-tunnel".into(),
            addr,
            "token".into(),
            vec![],
            None,
        );

        store.add(session).unwrap();
        assert_eq!(store.count(), 1);
        assert!(store.get(&id).is_some());
        assert!(store.get_by_tunnel_id("shard-tunnel").is_some());

        store.remove(&id);
        assert_eq!(store.count(), 0);
        assert!(store.get_by_tunnel_id("shard-tunnel").is_none());
    }

    /// Number of concurrent handshakes racing on the same tunnel_id.
    const RACE_THREADS: usize = 32;

    #[test]
    fn test_concurrent_add_same_tunnel_id_no_orphan() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let store = SessionStore::new();
        let addr = "127.0.0.1:1234".parse().unwrap();
        let barrier = Arc::new(Barrier::new(RACE_THREADS));

        let handles: Vec<_> = (0..RACE_THREADS)
            .map(|_| {
                let store = store.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let session = Session::new(
                        Uuid::new_v4(),
                        "race-tunnel".into(),
                        addr,
                        "token".into(),
                        vec![],
                        None,
                    );
                    // Release all threads simultaneously to force a true race.
                    barrier.wait();
                    store.add(session)
                })
            })
            .collect();

        let mut ok = 0usize;
        let mut conflicts = 0usize;
        for handle in handles {
            match handle.join().expect("thread panicked") {
                Ok(()) => ok += 1,
                Err(SessionStoreError::TunnelIdAlreadyExists(_)) => conflicts += 1,
            }
        }

        assert_eq!(ok, 1, "exactly one registration should succeed");
        assert_eq!(conflicts, RACE_THREADS - 1, "all others must conflict");
        assert_eq!(store.count(), 1, "no orphaned session should remain");
    }

    #[test]
    fn test_sharded_concurrent_add_same_tunnel_id_no_orphan() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let store = ShardedSessionStore::with_shards(4);
        let addr = "127.0.0.1:1234".parse().unwrap();
        let barrier = Arc::new(Barrier::new(RACE_THREADS));

        let handles: Vec<_> = (0..RACE_THREADS)
            .map(|_| {
                let store = store.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let session = Session::new(
                        Uuid::new_v4(),
                        "race-tunnel".into(),
                        addr,
                        "token".into(),
                        vec![],
                        None,
                    );
                    barrier.wait();
                    store.add(session)
                })
            })
            .collect();

        let mut ok = 0usize;
        let mut conflicts = 0usize;
        for handle in handles {
            match handle.join().expect("thread panicked") {
                Ok(()) => ok += 1,
                Err(SessionStoreError::TunnelIdAlreadyExists(_)) => conflicts += 1,
            }
        }

        assert_eq!(ok, 1, "exactly one registration should succeed");
        assert_eq!(conflicts, RACE_THREADS - 1, "all others must conflict");
        assert_eq!(store.count(), 1, "no orphaned session should remain");
    }
}
