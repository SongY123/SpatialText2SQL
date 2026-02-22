PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    last_login DATETIME NULL,
    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS database_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL CHECK (type IN ('Spatial', 'Postgis')),
    url TEXT NOT NULL,
    db_username TEXT NULL,
    db_password TEXT NULL,
    schema TEXT NOT NULL DEFAULT '[]',
    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);
CREATE INDEX IF NOT EXISTS idx_users_status ON users (status);
CREATE INDEX IF NOT EXISTS idx_database_links_user_id ON database_links (user_id);
CREATE INDEX IF NOT EXISTS idx_database_links_type ON database_links (type);

CREATE TABLE IF NOT EXISTS chat_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chat_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    request_id INTEGER NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    agent_name TEXT NULL,
    content TEXT NOT NULL,
    context_json TEXT NULL,
    feedback TEXT NULL CHECK (feedback IS NULL OR feedback IN ('like', 'dislike')),
    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (chat_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_user_id ON chat_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_chat_history_chat_id ON chat_history (chat_id);
CREATE INDEX IF NOT EXISTS idx_chat_history_request_id ON chat_history (request_id);
CREATE INDEX IF NOT EXISTS idx_chat_history_agent_name ON chat_history (agent_name);
CREATE INDEX IF NOT EXISTS idx_chat_history_feedback ON chat_history (feedback);

CREATE TABLE IF NOT EXISTS sql_execution_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    chat_id INTEGER NULL,
    database_id INTEGER NOT NULL,
    execute_status TEXT NOT NULL CHECK (execute_status IN ('success', 'failure')),
    sql_text TEXT NULL,
    execution_time_ms INTEGER NOT NULL DEFAULT 0 CHECK (execution_time_ms >= 0),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (chat_id) REFERENCES chat_sessions(id) ON DELETE SET NULL,
    FOREIGN KEY (database_id) REFERENCES database_links(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_user_id ON sql_execution_logs (user_id);
CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_chat_id ON sql_execution_logs (chat_id);
CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_database_id ON sql_execution_logs (database_id);
CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_status ON sql_execution_logs (execute_status);
CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_insert_time ON sql_execution_logs (insert_time);

CREATE TRIGGER IF NOT EXISTS trg_users_update_time
AFTER UPDATE ON users
FOR EACH ROW
WHEN NEW.update_time = OLD.update_time
BEGIN
    UPDATE users
    SET update_time = CURRENT_TIMESTAMP
    WHERE id = OLD.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_database_links_update_time
AFTER UPDATE ON database_links
FOR EACH ROW
WHEN NEW.update_time = OLD.update_time
BEGIN
    UPDATE database_links
    SET update_time = CURRENT_TIMESTAMP
    WHERE id = OLD.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_chat_sessions_update_time
AFTER UPDATE ON chat_sessions
FOR EACH ROW
WHEN NEW.update_time = OLD.update_time
BEGIN
    UPDATE chat_sessions
    SET update_time = CURRENT_TIMESTAMP
    WHERE id = OLD.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_chat_history_update_time
AFTER UPDATE ON chat_history
FOR EACH ROW
WHEN NEW.update_time = OLD.update_time
BEGIN
    UPDATE chat_history
    SET update_time = CURRENT_TIMESTAMP
    WHERE id = OLD.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_sql_execution_logs_update_time
AFTER UPDATE ON sql_execution_logs
FOR EACH ROW
WHEN NEW.update_time = OLD.update_time
BEGIN
    UPDATE sql_execution_logs
    SET update_time = CURRENT_TIMESTAMP
    WHERE id = OLD.id;
END;

INSERT OR IGNORE INTO users (username, password, role)
VALUES ('user', 'user', 'user');

INSERT OR IGNORE INTO users (username, password, role)
VALUES ('admin', 'admin', 'admin');
