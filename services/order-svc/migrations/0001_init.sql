CREATE TABLE menu_items (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT      NOT NULL,
    price_cents BIGINT    NOT NULL CHECK (price_cents > 0)
);

INSERT INTO menu_items (name, price_cents) VALUES
    ('Маргарита',        79000),
    ('Пепперони',        89000),
    ('Четыре сыра',      95000),
    ('Цезарь с курицей', 52000),
    ('Том-ям',           61000),
    ('Лимонад',          19000);

CREATE TABLE orders (
    id          UUID        PRIMARY KEY,
    client_id   TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    total_cents BIGINT      NOT NULL,
    lines       JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX orders_created_at_idx ON orders (created_at);
