CREATE TABLE IF NOT EXISTS vm_meta.layer_registry
(
  identity varchar(100) PRIMARY KEY,
  active boolean DEFAULT TRUE NOT NULL,
  relation varchar(5),
  geom_type varchar(20),
  pkey varchar(30),
  status varchar(12),
  err boolean DEFAULT FALSE NOT NULL,
  sup varchar(10),
  sup_ver bigint,
  sup_date timestamp,
	sup_type varchar(10),
	md_uuid varchar(36),
  extradata json,
  edit_date timestamp DEFAULT NOW() NOT NULL
);