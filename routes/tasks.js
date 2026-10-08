'use strict';

const crypto = require('crypto');
const express = require('express');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const {
  canCreatePushTasks, canManagePushTask, canReviewPushTask, canUsePushTasks,
  modeOf, normalizePushTaskInput, verifyJobSignature,
} = require('../lib/pushTaskPolicy');
const { processPushTaskEscalations } = require('../services/pushTaskEscalationService');

const router = express.Router();
const photoParser = express.raw({ type: ['image/jpeg', 'image/png', 'image/webp'], limit: '5mb' });
const mode = () => modeOf(process.env.PUSH_TASK_MODE);
const clean = (value) => typeof value === 'string' ? value.trim() : '';
const hasGlobalTaskView = (user) => user?.role === 'Admin' || (mode() === 'full' && user?.role === 'Geschäftsführer');

function requireFeature(req, res, next) {
  if (!canUsePushTasks(req.user?.role, mode())) return res.status(403).json({ message: 'Pushtasks sind während des Piloten nur für Admin und Supervisor freigeschaltet.' });
  next();
}

async function withTransaction(work) {
  const client = await pool.connect();
  try { await client.query('BEGIN'); const value = await work(client); await client.query('COMMIT'); return value; }
  catch (error) { try { await client.query('ROLLBACK'); } catch (_) {} throw error; }
  finally { client.release(); }
}

const taskSelect = `SELECT t.*,cu.name AS creator_name,cu.role AS creator_role,
  COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'id',a.id,'assignee_user_id',a.assignee_user_id,'assignee_name',u.name,'assignee_role',u.role,
    'status',a.status,'escalation_level',a.escalation_level,'escalation_snoozed_until',a.escalation_snoozed_until,
    'seen_at',a.seen_at,'submitted_at',a.submitted_at,'submission_comment',a.submission_comment,
    'approved_at',a.approved_at,'rejected_at',a.rejected_at,'review_comment',a.review_comment,
    'evidence',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',e.id,'file_name',e.file_name,'mime_type',e.mime_type,'size_bytes',e.size_bytes,'created_at',e.created_at)) FROM core.push_task_evidence e WHERE e.assignment_id=a.id),'[]'::jsonb)
  ) ORDER BY u.name) FROM core.push_task_assignments a JOIN public.users u ON u.id=a.assignee_user_id
    WHERE a.task_id=t.id AND ($2::boolean OR t.created_by_user_id=$1 OR a.assignee_user_id=$1)),'[]'::jsonb) AS assignments,
  COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'id',ev.id,'event_type',ev.event_type,'actor_name',actor.name,'details',ev.details,'created_at',ev.created_at
  ) ORDER BY ev.created_at DESC) FROM core.push_task_events ev LEFT JOIN public.users actor ON actor.id=ev.actor_user_id
    WHERE ev.task_id=t.id AND ($2::boolean OR t.created_by_user_id=$1 OR ev.assignment_id IN
      (SELECT own_event.id FROM core.push_task_assignments own_event WHERE own_event.task_id=t.id AND own_event.assignee_user_id=$1))),'[]'::jsonb) AS events
  FROM core.push_tasks t JOIN public.users cu ON cu.id=t.created_by_user_id`;

async function loadTask(id, user) {
  const result = await pool.query(`${taskSelect} WHERE t.id=$3 AND ($2::boolean OR t.created_by_user_id=$1 OR EXISTS
    (SELECT 1 FROM core.push_task_assignments mine WHERE mine.task_id=t.id AND mine.assignee_user_id=$1))`,
  [user.id,hasGlobalTaskView(user),id]);
  return result.rows[0] || null;
}

router.post('/internal/process-escalations', async (req, res) => {
  if (!verifyJobSignature({ secret:process.env.TASK_JOB_SECRET,timestamp:req.get('x-task-job-timestamp'),signature:req.get('x-task-job-signature'),method:req.method,path:req.originalUrl.split('?')[0] })) {
    return res.status(401).json({ message: 'Ungültige Job-Signatur.' });
  }
  try { return res.json({ status:'ok',result:await processPushTaskEscalations({ pool }) }); }
  catch (error) { console.error('Pushtask-Eskalationsjob:',error); return res.status(500).json({ message:'Eskalationsjob fehlgeschlagen.' }); }
});

router.use(verifyToken(), requireFeature);

router.get('/users', async (req,res) => {
  const result = await pool.query(`SELECT id,name,role,filiale FROM public.users ORDER BY name`);
  res.json({ users:result.rows.filter((user)=>canUsePushTasks(user.role,mode())) });
});

router.get('/notifications', async (req,res) => {
  const result = await pool.query(`SELECT id,task_id,assignment_id,escalation_level,kind,title,body,read_at,created_at
    FROM core.push_task_notifications WHERE user_id=$1 ORDER BY read_at NULLS FIRST,created_at DESC LIMIT 100`, [req.user.id]);
  res.json({ notifications:result.rows });
});

router.post('/notifications/:id/read', async (req,res) => {
  const result = await pool.query(`UPDATE core.push_task_notifications SET read_at=COALESCE(read_at,now()) WHERE id=$1 AND user_id=$2 RETURNING *`, [req.params.id,req.user.id]);
  if (!result.rowCount) return res.status(404).json({ message:'Benachrichtigung nicht gefunden.' });
  res.json({ notification:result.rows[0] });
});

router.get('/', async (req,res) => {
  const result = await pool.query(`${taskSelect} WHERE $2::boolean OR t.created_by_user_id=$1 OR EXISTS
    (SELECT 1 FROM core.push_task_assignments mine WHERE mine.task_id=t.id AND mine.assignee_user_id=$1)
    ORDER BY CASE t.status WHEN 'active' THEN 0 ELSE 1 END,t.due_at,t.created_at DESC LIMIT 300`, [req.user.id,hasGlobalTaskView(req.user)]);
  res.json({ mode:mode(),tasks:result.rows });
});

router.post('/', async (req,res) => {
  if (!canCreatePushTasks(req.user.role,mode())) return res.status(403).json({ message:'Keine Berechtigung zum Erstellen.' });
  try {
    const input = normalizePushTaskInput(req.body);
    const users = await pool.query(`SELECT id,name,role FROM public.users WHERE id=ANY($1::int[])`, [input.assigneeIds]);
    if (users.rowCount !== input.assigneeIds.length || users.rows.some((user)=>!canUsePushTasks(user.role,mode()))) {
      return res.status(400).json({ message:'Im Piloten dürfen ausschließlich aktive Admin- und Supervisor-Benutzer ausgewählt werden.' });
    }
    const id = await withTransaction(async (client) => {
      const created = await client.query(`INSERT INTO core.push_tasks
        (task_type,title,description,priority,proof_mode,due_at,reminder_at,urgent_at,hard_escalation_at,created_by_user_id)
        VALUES ('central_push',$1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
      [input.title,input.description,input.priority,input.proofMode,input.dueAt,input.reminderAt,input.urgentAt,input.hardAt,req.user.id]);
      for (const userId of input.assigneeIds) await client.query(`INSERT INTO core.push_task_assignments (task_id,assignee_user_id) VALUES ($1,$2)`, [created.rows[0].id,userId]);
      await client.query(`INSERT INTO core.push_task_events (task_id,event_type,actor_user_id,details) VALUES ($1,'created',$2,$3)`, [created.rows[0].id,req.user.id,{assignee_user_ids:input.assigneeIds}]);
      return created.rows[0].id;
    });
    res.status(201).json({ task:await loadTask(id,req.user) });
  } catch (error) { res.status(error.statusCode||500).json({ message:error.statusCode?error.message:'Task konnte nicht erstellt werden.' }); }
});

router.get('/:id', async (req,res) => {
  const task = await loadTask(req.params.id,req.user); if (!task) return res.status(404).json({ message:'Task nicht gefunden.' });
  res.json({ task });
});

router.patch('/:id', async (req,res) => {
  const existing = await loadTask(req.params.id,req.user); if (!existing) return res.status(404).json({ message:'Task nicht gefunden.' });
  if (!canManagePushTask(existing,req.user,mode())) return res.status(403).json({ message:'Nur Ersteller, Admin oder Geschäftsführer dürfen bearbeiten.' });
  if (existing.status !== 'active') return res.status(409).json({ message:'Abgeschlossene oder abgebrochene Tasks sind schreibgeschützt.' });
  try {
    const input = normalizePushTaskInput({ ...req.body,assignee_user_ids:existing.assignments.map((item)=>item.assignee_user_id) });
    await withTransaction(async (client) => {
      await client.query(`UPDATE core.push_tasks SET title=$2,description=$3,priority=$4,proof_mode=$5,due_at=$6,reminder_at=$7,urgent_at=$8,hard_escalation_at=$9,updated_at=now() WHERE id=$1`,
      [existing.id,input.title,input.description,input.priority,input.proofMode,input.dueAt,input.reminderAt,input.urgentAt,input.hardAt]);
      await client.query(`INSERT INTO core.push_task_events (task_id,event_type,actor_user_id,details) VALUES ($1,'updated',$2,$3)`, [existing.id,req.user.id,{reason:clean(req.body.reason)||null}]);
    });
    res.json({ task:await loadTask(existing.id,req.user) });
  } catch (error) { res.status(error.statusCode||500).json({ message:error.statusCode?error.message:'Task konnte nicht gespeichert werden.' }); }
});

router.post('/:id/seen', async (req,res) => {
  const updated = await pool.query(`UPDATE core.push_task_assignments SET status=CASE WHEN status='open' THEN 'seen' ELSE status END,seen_at=COALESCE(seen_at,now()),updated_at=now()
    WHERE task_id=$1 AND assignee_user_id=$2 AND status IN ('open','seen','rejected') RETURNING id`, [req.params.id,req.user.id]);
  if (!updated.rowCount) return res.status(404).json({ message:'Eigene offene Zuweisung nicht gefunden.' });
  await pool.query(`INSERT INTO core.push_task_events (task_id,assignment_id,event_type,actor_user_id) VALUES ($1,$2,'seen',$3)`, [req.params.id,updated.rows[0].id,req.user.id]);
  res.json({ task:await loadTask(req.params.id,req.user) });
});

router.post('/:id/assignments/:assignmentId/photo', photoParser, async (req,res) => {
  if (!canUsePushTasks(req.user?.role,mode())) return res.status(403).json({ message:'Kein Zugriff.' });
  const assignment = await pool.query(`SELECT a.id,a.task_id,a.assignee_user_id FROM core.push_task_assignments a JOIN core.push_tasks t ON t.id=a.task_id
    WHERE a.id=$1 AND a.task_id=$2 AND t.status='active'`, [req.params.assignmentId,req.params.id]);
  if (!assignment.rowCount || Number(assignment.rows[0].assignee_user_id)!==Number(req.user.id)) return res.status(403).json({ message:'Foto darf nur für die eigene Zuweisung hochgeladen werden.' });
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ message:'Bilddatei fehlt.' });
  const mime = req.get('content-type')?.split(';')[0]; const fileName = clean(decodeURIComponent(req.get('x-file-name')||'nachweis.jpg')).slice(0,200);
  const result = await pool.query(`INSERT INTO core.push_task_evidence (assignment_id,evidence_type,file_name,mime_type,size_bytes,sha256,content,uploaded_by_user_id)
    VALUES ($1,'photo',$2,$3,$4,$5,$6,$7) RETURNING id,file_name,mime_type,size_bytes,created_at`,
  [assignment.rows[0].id,fileName,mime,req.body.length,crypto.createHash('sha256').update(req.body).digest('hex'),req.body,req.user.id]);
  await pool.query(`INSERT INTO core.push_task_events (task_id,assignment_id,event_type,actor_user_id,details) VALUES ($1,$2,'photo_uploaded',$3,$4)`, [req.params.id,assignment.rows[0].id,req.user.id,{evidence_id:result.rows[0].id}]);
  res.status(201).json({ evidence:result.rows[0] });
});

router.get('/:id/evidence/:evidenceId', async (req,res) => {
  const task = await loadTask(req.params.id,req.user); if (!task) return res.status(404).json({ message:'Task nicht gefunden.' });
  const result = await pool.query(`SELECT file_name,mime_type,content FROM core.push_task_evidence e JOIN core.push_task_assignments a ON a.id=e.assignment_id WHERE e.id=$1 AND a.task_id=$2`, [req.params.evidenceId,req.params.id]);
  if (!result.rowCount) return res.status(404).json({ message:'Nachweis nicht gefunden.' });
  res.type(result.rows[0].mime_type).set('Content-Disposition',`inline; filename="${result.rows[0].file_name.replace(/["\\]/g,'_')}"`).send(result.rows[0].content);
});

router.post('/:id/submit', async (req,res) => {
  const assignment = await pool.query(`SELECT a.*,t.proof_mode,t.created_by_user_id,t.title FROM core.push_task_assignments a JOIN core.push_tasks t ON t.id=a.task_id
    WHERE a.task_id=$1 AND a.assignee_user_id=$2 AND t.status='active'`, [req.params.id,req.user.id]);
  if (!assignment.rowCount) return res.status(404).json({ message:'Eigene Zuweisung nicht gefunden.' }); const row = assignment.rows[0];
  if (row.proof_mode!=='confirm' && !(await pool.query(`SELECT 1 FROM core.push_task_evidence WHERE assignment_id=$1 LIMIT 1`,[row.id])).rowCount) return res.status(400).json({ message:'Für diese Task ist ein Foto erforderlich.' });
  const comment = clean(req.body?.comment);
  await withTransaction(async (client) => {
    await client.query(`UPDATE core.push_task_assignments SET status='submitted',submitted_at=now(),submission_comment=$2,review_comment=NULL,rejected_at=NULL,updated_at=now() WHERE id=$1`, [row.id,comment||null]);
    await client.query(`INSERT INTO core.push_task_events (task_id,assignment_id,event_type,actor_user_id,details) VALUES ($1,$2,'submitted',$3,$4)`, [req.params.id,row.id,req.user.id,{comment:comment||null}]);
    await client.query(`INSERT INTO core.push_task_notifications (task_id,assignment_id,user_id,escalation_level,kind,title,body) VALUES ($1,$2,$3,0,'approval_required',$4,$5)`, [req.params.id,row.id,row.created_by_user_id,`Abnahme erforderlich: ${row.title}`,`${req.user.name} hat die Erledigung eingereicht.`]);
  });
  res.json({ task:await loadTask(req.params.id,req.user) });
});

router.post('/:id/assignments/:assignmentId/review', async (req,res) => {
  const task = await loadTask(req.params.id,req.user); if (!task) return res.status(404).json({ message:'Task nicht gefunden.' });
  if (!canReviewPushTask(task,req.user)) return res.status(403).json({ message:'Nur der ursprüngliche Ersteller darf die Erledigung abnehmen.' });
  if (task.status !== 'active') return res.status(409).json({ message:'Diese Task ist bereits abgeschlossen oder abgebrochen.' });
  const action=clean(req.body?.action),reason=clean(req.body?.reason);
  if (!['approve','reject'].includes(action)||(action==='reject'&&!reason)) return res.status(400).json({ message:'Entscheidung ungültig; eine Ablehnung benötigt eine Begründung.' });
  try {
    await withTransaction(async (client) => {
      const changed = await client.query(`UPDATE core.push_task_assignments SET status=$3,approved_at=CASE WHEN $3='approved' THEN now() ELSE approved_at END,
        rejected_at=CASE WHEN $3='rejected' THEN now() ELSE NULL END,review_comment=$4,updated_at=now() WHERE id=$1 AND task_id=$2 AND status='submitted' RETURNING assignee_user_id`,
      [req.params.assignmentId,req.params.id,action==='approve'?'approved':'rejected',reason||null]);
      if (!changed.rowCount) throw Object.assign(new Error('Zuweisung wartet nicht auf Abnahme.'),{statusCode:409});
      await client.query(`INSERT INTO core.push_task_events (task_id,assignment_id,event_type,actor_user_id,details) VALUES ($1,$2,$3,$4,$5)`, [req.params.id,req.params.assignmentId,action==='approve'?'approved':'rejected',req.user.id,{reason:reason||null}]);
      await client.query(`INSERT INTO core.push_task_notifications (task_id,assignment_id,user_id,escalation_level,kind,title,body) VALUES ($1,$2,$3,0,$4,$5,$6)`,
      [req.params.id,req.params.assignmentId,changed.rows[0].assignee_user_id,action==='approve'?'approved':'rejected',`${action==='approve'?'Bestätigt':'Abgelehnt'}: ${task.title}`,reason||'Die Erledigung wurde bestätigt.']);
      if (!(await client.query(`SELECT 1 FROM core.push_task_assignments WHERE task_id=$1 AND status<>'approved' LIMIT 1`,[req.params.id])).rowCount) await client.query(`UPDATE core.push_tasks SET status='completed',completed_at=now(),updated_at=now() WHERE id=$1`,[req.params.id]);
    });
    res.json({ task:await loadTask(req.params.id,req.user) });
  } catch (error) { res.status(error.statusCode||500).json({ message:error.statusCode?error.message:'Abnahme fehlgeschlagen.' }); }
});

router.post('/:id/deescalate', async (req,res) => {
  const task=await loadTask(req.params.id,req.user); if(!task)return res.status(404).json({message:'Task nicht gefunden.'});
  if(!canManagePushTask(task,req.user,mode()))return res.status(403).json({message:'Keine Berechtigung zur Deeskalation.'});
  if(task.status!=='active')return res.status(409).json({message:'Nur aktive Tasks können deeskaliert werden.'});
  const reason=clean(req.body?.reason),until=new Date(req.body?.snoozed_until),level=Number(req.body?.level);
  if(!reason||reason.length>1000||Number.isNaN(until.getTime())||until<=new Date()||!Number.isInteger(level)||level<0||level>2)return res.status(400).json({message:'Grund, zukünftiges Wiedervorlagedatum und Zielstufe 0–2 sind erforderlich.'});
  const ids=Array.isArray(req.body?.assignment_ids)?req.body.assignment_ids:task.assignments.map((item)=>item.id);
  await withTransaction(async(client)=>{await client.query(`UPDATE core.push_task_assignments SET escalation_level=$3,escalation_snoozed_until=$4,updated_at=now() WHERE task_id=$1 AND id=ANY($2::uuid[])`,[task.id,ids,level,until.toISOString()]);
    await client.query(`INSERT INTO core.push_task_events (task_id,event_type,actor_user_id,details) VALUES ($1,'deescalated',$2,$3)`,[task.id,req.user.id,{reason,level,snoozed_until:until.toISOString(),assignment_ids:ids}]);});
  res.json({task:await loadTask(task.id,req.user)});
});

router.post('/:id/cancel', async(req,res)=>{
  const task=await loadTask(req.params.id,req.user);if(!task)return res.status(404).json({message:'Task nicht gefunden.'});
  if(!canManagePushTask(task,req.user,mode()))return res.status(403).json({message:'Keine Berechtigung zum Abbrechen.'});
  if(task.status!=='active')return res.status(409).json({message:'Nur aktive Tasks können abgebrochen werden.'});
  const reason=clean(req.body?.reason);if(!reason)return res.status(400).json({message:'Begründung ist erforderlich.'});
  await withTransaction(async(client)=>{await client.query(`UPDATE core.push_tasks SET status='cancelled',cancelled_at=now(),updated_at=now() WHERE id=$1`,[task.id]);
    await client.query(`INSERT INTO core.push_task_events (task_id,event_type,actor_user_id,details) VALUES ($1,'cancelled',$2,$3)`,[task.id,req.user.id,{reason}]);});
  res.json({task:await loadTask(task.id,req.user)});
});

module.exports=router;
