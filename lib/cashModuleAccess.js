const CENTRAL_ROLES = Object.freeze(['Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1', 'Manager']);
const CASH_CORRECTION_ROLES = Object.freeze(['Filialleitung', ...CENTRAL_ROLES]);

function isCentralRole(role) {
  return CENTRAL_ROLES.includes(String(role || '').trim());
}

function canUseCashModule(user = {}) {
  return isCentralRole(user.role) || String(user.filiale || '').trim().toLocaleLowerCase('de-DE') === 'vreden';
}

function canCorrectCashClosing(role) {
  return CASH_CORRECTION_ROLES.includes(String(role || '').trim());
}

function requestedBranch(req) {
  return String(req.get?.('x-cash-branch') || '').trim();
}

async function resolveCashBranch(queryable, req) {
  if (!canUseCashModule(req.user)) {
    throw Object.assign(new Error('Die Tageskassenabrechnung ist während der DEV-Phase nur für Vreden und zentrale Rollen freigegeben.'), { statusCode: 403 });
  }

  const assigned = String(req.user?.filiale || '').trim();
  let branchName = assigned;
  if (assigned.toLocaleLowerCase('de-DE') === 'alle') {
    if (!isCentralRole(req.user?.role)) {
      throw Object.assign(new Error('Für die Tageskassenabrechnung fehlt eine eindeutige Filialzuweisung.'), { statusCode: 403 });
    }
    branchName = requestedBranch(req);
    if (!branchName || branchName.toLocaleLowerCase('de-DE') === 'alle') {
      throw Object.assign(new Error('Bitte zuerst eine echte Filiale für diesen Kassenvorgang auswählen.'), { statusCode: 400 });
    }
  } else if (requestedBranch(req) && requestedBranch(req) !== assigned) {
    throw Object.assign(new Error('Die gewählte Filiale stimmt nicht mit deiner festen Filialzuweisung überein.'), { statusCode: 403 });
  }

  const result = await queryable.query(
    `SELECT id,name FROM public.filialen
     WHERE name=$1 AND aktiv=true AND LOWER(name)<>'alle' LIMIT 1`,
    [branchName]
  );
  if (!result.rows[0]) {
    throw Object.assign(new Error('Die gewählte Filiale ist nicht aktiv oder wurde nicht gefunden.'), { statusCode: 403 });
  }
  return result.rows[0];
}

module.exports = { CENTRAL_ROLES, CASH_CORRECTION_ROLES, canCorrectCashClosing, canUseCashModule, isCentralRole, resolveCashBranch };
