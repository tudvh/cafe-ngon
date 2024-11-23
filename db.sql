CREATE TABLE IF NOT EXISTS media_data (
    id CHAR(36) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    resource_id VARCHAR(255) UNIQUE NOT NULL,
    resource_url TEXT NOT NULL,
    resource_type INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_name (user_name),
    INDEX idx_resource_type (resource_type)
);

CREATE TABLE IF NOT EXISTS processed_users (
    user_name VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
