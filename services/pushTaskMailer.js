'use strict';

const nodemailer = require('nodemailer');

async function sendPushTaskMail({ to, subject, text }, env = process.env) {
  if (env.SAVEROOM_RUNTIME_MODE === 'LOCAL_MOCK_RUNTIME' || env.MAIL_MODE === 'disabled') {
    return { status: 'blocked', message: 'Runtime blockiert SMTP-Versand' };
  }
  const mode = env.MAIL_MODE || 'test';
  const recipient = mode === 'test' ? env.MAIL_TEST_RECIPIENT : to;
  if (!recipient || !env.MAIL_HOST || !env.MAIL_USER || !env.MAIL_PASS) return { status: 'failed', message: 'SMTP-Konfiguration unvollständig' };
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(String(recipient))) return { status: 'failed', message: 'Ungültige Empfängeradresse' };
  try {
    const port = Number(env.MAIL_PORT || 587);
    const transporter = nodemailer.createTransport({
      host: env.MAIL_HOST, port, secure: port === 465, requireTLS: port === 587,
      auth: { user: env.MAIL_USER, pass: env.MAIL_PASS }, tls: { rejectUnauthorized: true },
    });
    await transporter.sendMail({ from: `"Neufeld Pushtasks" <${env.MAIL_USER}>`, to: recipient, subject, text });
    return { status: 'sent', message: 'E-Mail versendet' };
  } catch (error) { return { status: 'failed', message: error.message }; }
}

module.exports = { sendPushTaskMail };
