-- Additive Pushtask foundation. Legacy core.tasks remains untouched.

CREATE TABLE IF NOT EXISTS core.push_tasks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_type text NOT NULL DEFAULT 'central_push' CHECK (task_type IN ('central_push', 'branch_todo')),
  title text NOT NULL CHECK (btrim(title) <> '' AND length(title) <= 160),
  description text NOT NULL CHECK (btrim(description) <> '' AND length(description) <= 5000),
  priority text NOT NULL DEFAULT 'normal' CHECK (priority IN ('normal', 'high', 'critical')),
  proof_mode text NOT NULL DEFAULT 'confirm' CHECK (proof_mode IN ('confirm', 'photo', 'both')),
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'completed', 'cancelled')),
  due_at timestamptz NOT NULL,
  reminder_at timestamptz NOT NULL,
  urgent_at timestamptz NOT NULL,
  hard_escalation_at timestamptz NOT NULL,
  created_by_user_id integer NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  cancelled_at timestamptz,
  CHECK (reminder_at <= urgent_at AND urgent_at <= hard_escalation_at)
);

CREATE TABLE IF NOT EXISTS core.push_task_assignments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id uuid NOT NULL REFERENCES core.push_tasks(id) ON DELETE CASCADE,
  assignee_user_id integer NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'seen', 'submitted', 'approved', 'rejected')),
  escalation_level integer NOT NULL DEFAULT 0 CHECK (escalation_level BETWEEN 0 AND 3),
  escalation_snoozed_until timestamptz,
  seen_at timestamptz,
  submitted_at timestamptz,
  submission_comment text,
  approved_at timestamptz,
  rejected_at timestamptz,
  review_comment text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (task_id, assignee_user_id)
);

CREATE TABLE IF NOT EXISTS core.push_task_evidence (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  assignment_id uuid NOT NULL REFERENCES core.push_task_assignments(id) ON DELETE CASCADE,
  evidence_type text NOT NULL CHECK (evidence_type IN ('photo')),
  file_name text NOT NULL,
  mime_type text NOT NULL CHECK (mime_type IN ('image/jpeg', 'image/png', 'image/webp')),
  size_bytes integer NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 5242880),
  sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  content bytea NOT NULL,
  uploaded_by_user_id integer NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.push_task_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id uuid NOT NULL REFERENCES core.push_tasks(id) ON DELETE CASCADE,
  assignment_id uuid REFERENCES core.push_task_assignments(id) ON DELETE CASCADE,
  event_type text NOT NULL,
  actor_user_id integer REFERENCES public.users(id) ON DELETE SET NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.push_task_notifications (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id uuid NOT NULL REFERENCES core.push_tasks(id) ON DELETE CASCADE,
  assignment_id uuid REFERENCES core.push_task_assignments(id) ON DELETE CASCADE,
  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  escalation_level integer NOT NULL CHECK (escalation_level BETWEEN 0 AND 3),
  kind text NOT NULL CHECK (kind IN ('escalation', 'approval_required', 'rejected', 'approved')),
  title text NOT NULL,
  body text NOT NULL,
  read_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.push_task_dispatches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id uuid NOT NULL REFERENCES core.push_tasks(id) ON DELETE CASCADE,
  assignment_id uuid NOT NULL REFERENCES core.push_task_assignments(id) ON DELETE CASCADE,
  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  escalation_level integer NOT NULL CHECK (escalation_level BETWEEN 1 AND 3),
  channel text NOT NULL CHECK (channel IN ('in_app', 'email')),
  status text NOT NULL CHECK (status IN ('pending', 'sent', 'blocked', 'failed')),
  recipient text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz,
  UNIQUE (assignment_id, user_id, escalation_level, channel)
);

CREATE INDEX IF NOT EXISTS idx_push_tasks_creator_status ON core.push_tasks (created_by_user_id, status, due_at);
CREATE INDEX IF NOT EXISTS idx_push_task_assignments_user_status ON core.push_task_assignments (assignee_user_id, status);
CREATE INDEX IF NOT EXISTS idx_push_task_assignments_escalation ON core.push_task_assignments (escalation_level, escalation_snoozed_until);
CREATE INDEX IF NOT EXISTS idx_push_task_events_task ON core.push_task_events (task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_push_task_notifications_user ON core.push_task_notifications (user_id, read_at, created_at DESC);

-- Runtime privileges follow the existing production role and schema grants.
