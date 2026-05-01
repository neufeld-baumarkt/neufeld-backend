const nodemailer = require('nodemailer');

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

async function sendOrderMail({ subject, text, to }) {
  try {
    const config = getMailConfig();

    if (!config.host || !config.user || !config.pass) {
      console.warn('MAIL: SMTP-Konfiguration unvollständig');
      return;
    }

    const finalRecipient =
      config.mode === 'test'
        ? config.testRecipient
        : to;

    if (!finalRecipient) {
      console.warn('MAIL: Kein Empfänger definiert');
      return;
    }

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
      to: finalRecipient,
      subject,
      text,
    });

    console.log(`MAIL: erfolgreich gesendet an ${finalRecipient}`);
  } catch (err) {
    console.error('MAIL ERROR:', err.message);
    console.error('MAIL ERROR CODE:', err.code);
    console.error('MAIL ERROR COMMAND:', err.command);
  }
}

module.exports = {
  sendOrderMail,
};