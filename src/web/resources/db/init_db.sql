PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS database_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_database_links_user_id ON database_links (user_id);
CREATE INDEX IF NOT EXISTS idx_database_links_type ON database_links (type);

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
