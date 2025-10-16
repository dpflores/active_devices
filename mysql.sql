CREATE TABLE device_status_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL UNIQUE,         -- sigue siendo único
    online_count INT NOT NULL,
    offline_count INT NOT NULL,
    filename VARCHAR(255) NOT NULL
);