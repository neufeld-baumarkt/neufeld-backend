const nodemailer = require('nodemailer');
const { validateMailEnvelope } = require('../lib/orderEmailPolicy');

function getMailConfig() {
  const config = {
    mode: process.env.MAIL_MODE || 'test',
    host: process.env.MAIL_HOST,
    port: Number(process.env.MAIL_PORT || 587),
    user: process.env.MAIL_USER,
    pass: process.env.MAIL_PASS,
    testRecipient: process.env.MAIL_TEST_RECIPIENT,
  };

  console.log('MAIL CONFIG CHECK:', {
    mode: config.mode,
    host: config.host,
    port: config.port,
    user: config.user,
    passLength: config.pass ? config.pass.length : null,
    testRecipient: config.testRecipient,
  });

  return config;
}

async function sendOrderMail({ subject, text, to, cc = [], attachments = [] }) {
  try {
    const config = getMailConfig();

    if (!config.host || !config.user || !config.pass) {
      console.warn('MAIL: SMTP-Konfiguration unvollständig');
      return { status: 'failed', message: 'SMTP-Konfiguration unvollständig' };
    }

    const envelope = validateMailEnvelope({
      mode: config.mode,
      sender: config.user,
      to,
      cc,
      testRecipient: config.testRecipient,
    });

    const transporter = nodemailer.createTransport({
      host: config.host,
      port: config.port,
      secure: config.port === 465,
      requireTLS: config.port === 587,
      auth: {
        user: config.user,
        pass: config.pass,
      },
      tls: {
        rejectUnauthorized: true,
      },
    });

    await transporter.sendMail({
      from: `"Neufeld Bestellungen" <${config.user}>`,
      to: envelope.to,
      cc: envelope.cc,
      subject,
      text,
      attachments,
    });

    console.log(`MAIL: erfolgreich gesendet an ${envelope.to.join(', ')}`);
    return { status: 'sent', message: 'E-Mail erfolgreich versendet' };
  } catch (err) {
    console.error('MAIL ERROR:', err.message);
    console.error('MAIL ERROR CODE:', err.code);
    console.error('MAIL ERROR COMMAND:', err.command);
    return { status: 'failed', message: err.message, code: err.code || null };
  }
}

module.exports = {
  sendOrderMail,
};