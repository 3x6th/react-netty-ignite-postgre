CREATE TABLE IF NOT EXISTS partner_flag (
                                            id              BIGSERIAL PRIMARY KEY,
                                            merchant_code   VARCHAR(64) NOT NULL,
                                            terminal_id     VARCHAR(64) NOT NULL,
                                            partner         BOOLEAN     NOT NULL,
                                            UNIQUE (merchant_code, terminal_id)
);
CREATE INDEX IF NOT EXISTS idx_partner_flag_m_t ON partner_flag(merchant_code, terminal_id)