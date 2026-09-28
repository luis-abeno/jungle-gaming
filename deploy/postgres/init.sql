CREATE ROLE wallet_app LOGIN PASSWORD 'wallet_app';
GRANT CONNECT ON DATABASE wallet TO wallet_app;
