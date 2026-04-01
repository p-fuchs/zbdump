CREATE TABLE public.dump_fixture (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id integer NOT NULL,
    external_uuid uuid NOT NULL,
    email text,
    display_name character varying(100) NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    score numeric(10,2),
    login_count integer NOT NULL DEFAULT 0,
    tags text[] NOT NULL DEFAULT '{}'::text[],
    profile jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone,
    birthday date,
    preferred_time time without time zone,
    last_seen_at timestamp without time zone,
    reminder_window interval,
    avatar bytea,
    ip_address inet,
    homepage text
);

CREATE UNIQUE INDEX dump_fixture_external_uuid_idx
    ON public.dump_fixture (external_uuid);

CREATE UNIQUE INDEX dump_fixture_lower_email_idx
    ON public.dump_fixture (lower(email))
    WHERE email IS NOT NULL;

CREATE INDEX dump_fixture_active_tenant_created_cover_idx
    ON public.dump_fixture (tenant_id, created_at DESC)
    INCLUDE (display_name, score, updated_at)
    WHERE is_active;

CREATE INDEX dump_fixture_profile_gin_idx
    ON public.dump_fixture
    USING gin (profile jsonb_path_ops);

INSERT INTO public.dump_fixture (
    tenant_id,
    external_uuid,
    email,
    display_name,
    is_active,
    score,
    login_count,
    tags,
    profile,
    created_at,
    updated_at,
    birthday,
    preferred_time,
    last_seen_at,
    reminder_window,
    avatar,
    ip_address,
    homepage
) VALUES
    (
        10,
        '00000000-0000-0000-0000-000000000001',
        'alice@example.com',
        'Alice Example',
        true,
        91.50,
        7,
        ARRAY['alpha', 'beta'],
        '{"role":"admin","preferences":{"theme":"light","alerts":true}}'::jsonb,
        '2024-01-02 03:04:05+00',
        '2024-01-03 09:15:00+00',
        '1990-05-17',
        '08:30:00',
        '2024-01-02 03:04:05',
        '2 hours 15 minutes',
        decode('DEADBEEF', 'hex'),
        '192.168.10.15',
        'https://alice.example.com'
    ),
    (
        20,
        '00000000-0000-0000-0000-000000000002',
        'bob.o''connor@example.org',
        'Bob O''Connor',
        true,
        77.25,
        15,
        ARRAY['customer', 'vip', 'newsletter'],
        '{"role":"member","stats":{"orders":12,"spend":345.67},"flags":["beta","priority"]}'::jsonb,
        '2024-06-10 08:30:00+00',
        '2024-06-12 18:45:00+00',
        '1985-09-03',
        '14:45:00',
        '2024-06-10 10:30:00',
        '1 day 30 minutes',
        decode('0123456789ABCDEF', 'hex'),
        '10.20.30.40',
        'https://shop.example.org'
    ),
    (
        30,
        '00000000-0000-0000-0000-000000000003',
        NULL,
        'Charlie Nulls',
        false,
        NULL,
        0,
        '{}'::text[],
        '{}'::jsonb,
        '2025-02-14 12:00:00+00',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '203.0.113.42',
        'https://portal.example.net'
    );
