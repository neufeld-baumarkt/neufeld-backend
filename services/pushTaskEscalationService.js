'use strict';

const { escalationAudience, escalationLevelFor, modeOf } = require('../lib/pushTaskPolicy');
const { sendPushTaskMail } = require('./pushTaskMailer');

async function processPushTaskEscalations({ pool, env = process.env, now = new Date(), mailer = sendPushTaskMail }) {
  const mode = modeOf(env.PUSH_TASK_MODE);
  const candidates = await pool.query(`SELECT a.id FROM core.push_task_assignments a JOIN core.push_tasks t ON t.id=a.task_id
    WHERE t.status='active' AND a.status NOT IN ('submitted','approved')
      AND (a.escalation_snoozed_until IS NULL OR a.escalation_snoozed_until <= $1) AND t.reminder_at <= $1
    ORDER BY t.due_at,a.id LIMIT 500`, [now]);
  const summary = { inspected: candidates.rowCount, escalated: 0, emails: { sent: 0, blocked: 0, failed: 0 } };
  for (const candidate of candidates.rows) {
    const client = await pool.connect();
    const emailJobs = [];
    try {
      await client.query('BEGIN');
      const locked = await client.query(`SELECT a.*,t.title,t.description,t.status AS task_status,t.created_by_user_id,
        t.reminder_at,t.urgent_at,t.hard_escalation_at,t.due_at,
        au.name AS assignee_name,au.email AS assignee_email,au.role AS assignee_role,au.active AS assignee_active,
        cu.name AS creator_name,cu.email AS creator_email,cu.role AS creator_role,cu.active AS creator_active
        FROM core.push_task_assignments a JOIN core.push_tasks t ON t.id=a.task_id
        JOIN public.users au ON au.id=a.assignee_user_id JOIN public.users cu ON cu.id=t.created_by_user_id
        WHERE a.id=$1 FOR UPDATE OF a`, [candidate.id]);
      const row = locked.rows[0];
      if (!row) { await client.query('ROLLBACK'); continue; }
      const targetLevel = escalationLevelFor({ ...row, status: row.task_status }, row, now);
      if (targetLevel <= Number(row.escalation_level || 0)) { await client.query('ROLLBACK'); continue; }
      const central = await client.query(`SELECT id,name,email,role,active FROM public.users WHERE active=true AND role=ANY($1::text[])`, [['Admin','Geschäftsführer']]);
      const audience = escalationAudience(targetLevel, {
        mode,
        assignee: { id: row.assignee_user_id,name:row.assignee_name,email:row.assignee_email,role:row.assignee_role,active:row.assignee_active },
        creator: { id: row.created_by_user_id,name:row.creator_name,email:row.creator_email,role:row.creator_role,active:row.creator_active },
        admins: central.rows.filter((u) => u.role === 'Admin'), directors: central.rows.filter((u) => u.role === 'Geschäftsführer'),
      });
      const channels = targetLevel === 1 ? ['in_app'] : targetLevel === 2 ? ['email'] : ['in_app','email'];
      const levelLabel = targetLevel === 1 ? 'Erinnerung' : targetLevel === 2 ? 'Dringende Fälligkeit' : 'Harte Eskalation';
      for (const person of audience) for (const channel of channels) {
        const dispatch = await client.query(`INSERT INTO core.push_task_dispatches
          (task_id,assignment_id,user_id,escalation_level,channel,status,recipient) VALUES ($1,$2,$3,$4,$5,'pending',$6)
          ON CONFLICT (assignment_id,user_id,escalation_level,channel) DO NOTHING RETURNING id`,
        [row.task_id,row.id,person.id,targetLevel,channel,channel === 'email' ? person.email : null]);
        if (!dispatch.rowCount) continue;
        if (channel === 'in_app') {
          await client.query(`INSERT INTO core.push_task_notifications (task_id,assignment_id,user_id,escalation_level,kind,title,body)
            VALUES ($1,$2,$3,$4,'escalation',$5,$6)`, [row.task_id,row.id,person.id,targetLevel,`${levelLabel}: ${row.title}`,`Fällig: ${new Date(row.due_at).toLocaleString('de-DE')}`]);
          await client.query(`UPDATE core.push_task_dispatches SET status='sent',sent_at=now() WHERE id=$1`, [dispatch.rows[0].id]);
        } else emailJobs.push({ id: dispatch.rows[0].id,to:person.email,subject:`[Pushtask Stufe ${targetLevel}] ${row.title}`,
          text:`${levelLabel}\n\n${row.title}\n${row.description}\n\nFällig: ${new Date(row.due_at).toLocaleString('de-DE')}` });
      }
      await client.query(`UPDATE core.push_task_assignments SET escalation_level=$2,updated_at=now() WHERE id=$1`, [row.id,targetLevel]);
      await client.query(`INSERT INTO core.push_task_events (task_id,assignment_id,event_type,details) VALUES ($1,$2,'escalated',$3)`,
        [row.task_id,row.id,{ level:targetLevel,channels,recipients:audience.map((x)=>x.id) }]);
      await client.query('COMMIT'); summary.escalated += 1;
    } catch (error) { try { await client.query('ROLLBACK'); } catch (_) {} throw error; }
    finally { client.release(); }
    for (const job of emailJobs) {
      const result = await mailer(job, env); const status = ['sent','blocked'].includes(result.status) ? result.status : 'failed';
      await pool.query(`UPDATE core.push_task_dispatches SET status=$2,error_message=$3,sent_at=CASE WHEN $2='sent' THEN now() ELSE sent_at END WHERE id=$1`, [job.id,status,result.message||null]);
      summary.emails[status] += 1;
    }
  }
  return summary;
}

module.exports = { processPushTaskEscalations };
