CREATE TABLE IF NOT EXISTS "order".order_supplier_article_audit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  article_id uuid NOT NULL REFERENCES "order".order_supplier_articles(id) ON DELETE RESTRICT,
  action text NOT NULL CHECK (action IN ('created', 'updated', 'activated', 'deactivated')),
  before_data jsonb,
  after_data jsonb NOT NULL,
  changed_by text NOT NULL,
  changed_at timestamp without time zone NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_supplier_article_audit_article
  ON "order".order_supplier_article_audit (article_id, changed_at DESC);
