require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const cron = require('node-cron');
const Bottleneck = require('bottleneck');
const express = require('express');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const xlsx = require('xlsx'); // Для чтения Excel-файлов (из буфера)

// ----------------------------------------------------------------------------------
// 1) ИНИЦИАЛИЗАЦИЯ БОТА, БАЗЫ И ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ----------------------------------------------------------------------------------

const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');

function getMoscowTimestamp() {
  return DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd HH:mm:ss');
}
function getMoscowDateTime() {
  return DateTime.now().setZone('Europe/Moscow');
}

// Создаём таблицы (если они ещё не созданы) – расширенная схема задач
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT,
    priority TEXT,
    department TEXT,
    issueType TEXT,
    dateAdded DATETIME,
    lastSent DATETIME,
    source TEXT,
    reporter TEXT,
    reporterLogin TEXT,
    assignee TEXT,
    assigneeLogin TEXT,
    status TEXT,

    -- Fastik (Office / Waiting for support + доступы не пустые)
    fastikNeeded INTEGER DEFAULT 0,           -- 0/1
    fastikFor TEXT,                           -- customfield_12418 (value)
    fastikRecipientsJson TEXT,                -- JSON array (display names)
    fastikAccessJson TEXT                     -- JSON array (what access)
  )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS approval_alerts (
      taskId TEXT PRIMARY KEY,
      lastStatus TEXT,
      lastSentAt DATETIME
    )
  `);

  db.run(`CREATE TABLE IF NOT EXISTS user_actions (
    username TEXT,
    taskId TEXT,
    action TEXT,
    timestamp DATETIME,
    FOREIGN KEY(taskId) REFERENCES tasks(id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS task_comments (
    taskId TEXT PRIMARY KEY,
    lastCommentId TEXT,
    assignee TEXT,
    FOREIGN KEY(taskId) REFERENCES tasks(id)
  )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS comment_cache (
      taskId TEXT,
      commentId TEXT,
      header TEXT,
      shortHtml TEXT,
      fullHtml TEXT,
      attachmentsJson TEXT,
      source TEXT,
      PRIMARY KEY (taskId, commentId)
    )
  `);
});

// Преобразование приоритета в эмодзи
function getPriorityEmoji(priority) {
  const emojis = {
    Blocker: '🚨',
    High: '🔴',
    Medium: '🟡',
    Low: '🟢'
  };
  return emojis[priority] || '';
}

// Удаляем префикс проекта из ключа, например "sxl-SUPPORT-19980" → "SUPPORT-19980"
function extractRealJiraKey(fullId) {
  if (fullId.startsWith('sxl-') || fullId.startsWith('betone-')) {
    const parts = fullId.split('-');
    parts.shift();
    return parts.join('-');
  }
  return fullId;
}

// Формирование URL для задачи Jira
function getTaskUrl(source, combinedId) {
  const realKey = extractRealJiraKey(combinedId);
  return `https://jira.${source}.team/browse/${realKey}`;
}


// Маппинги: Telegram username → ФИО и Jira логин
const usernameMappings = {
  "lipchinski": "Дмитрий Селиванов",
  "pr0spal": "Евгений Шушков",
  "fdhsudgjdgkdfg": "Даниил Маслов",
  "EuroKaufman": "Даниил Баратов",
  "Nikolay_Gonchar": "Николай Гончар",
  "KIRILlKxX": "Кирилл Атанизяов",
  "marysh353": "Даниил Марышев"
};
const jiraUserMappings = {
  "lipchinski": { "sxl": "d.selivanov", "betone": "dms" },
  "pr0spal": { "sxl": "e.shushkov", "betone": "es" },
  "fdhsudgjdgkdfg": { "sxl": "d.maslov", "betone": "dam" },
  "EuroKaufman": { "sxl": "d.baratov", "betone": "db" },
  "Nikolay_Gonchar": { "sxl": "n.gonchar", "betone": "ng" },
  "KIRILlKxX": { "sxl": "k.ataniyazov", "betone": "ka" },
  "marysh353": { "sxl": "d.maryshev", "betone": "dma" }
};

// ----------------------------------------------------------------------------------
// 2) EXPRESS-сервер и очистка папки attachments
// ----------------------------------------------------------------------------------

const app = express();
const ATTACHMENTS_DIR = path.join(__dirname, 'attachments');
if (!fs.existsSync(ATTACHMENTS_DIR)) { fs.mkdirSync(ATTACHMENTS_DIR); }
app.use('/attachments', express.static(ATTACHMENTS_DIR));
const EXPRESS_PORT = 3000;
app.listen(EXPRESS_PORT, () => {
  console.log(`Express server listening on port ${EXPRESS_PORT}`);
});
cron.schedule('0 3 1 * *', () => {
  console.log('[CRON] Удаляем старые файлы из attachments (ежемесячно)...');
  const now = Date.now();
  const cutoff = now - 24 * 60 * 60 * 1000;
  fs.readdir(ATTACHMENTS_DIR, (err, files) => {
    if (err) { console.error('Ошибка чтения папки attachments:', err); return; }
    files.forEach(file => {
      const filePath = path.join(ATTACHMENTS_DIR, file);
      fs.stat(filePath, (statErr, stats) => {
        if (statErr) { console.error('Ошибка fs.stat:', statErr); return; }
        if (stats.mtimeMs < cutoff) {
          fs.unlink(filePath, delErr => {
            if (delErr) { console.error('Ошибка удаления файла:', delErr); }
            else { console.log(`Файл ${file} удалён (старше суток)`); }
          });
        }
      });
    });
  });
}, { timezone: 'Europe/Moscow' });

// ----------------------------------------------------------------------------------
// 3) Сбор данных с Wiki и парсинг Excel-расписания
// ----------------------------------------------------------------------------------

const monthNamesRu = {
  'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
  'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
  'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
};
const PARENT_PAGE_ID = '55414233';
let pageMap = {};
const schedulesByKey = {};

async function buildPageMapForSchedules() {
  try {
    const baseUrl = 'https://wiki.sxl.team';
    const token = process.env.CONFLUENCE_API_TOKEN;
    const url = `${baseUrl}/rest/api/content/${PARENT_PAGE_ID}/child/page?limit=200`;
    const resp = await axios.get(url, {
      headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' }
    });
    if (!resp.data || !resp.data.results) throw new Error(`Не удалось прочитать дочерние страницы для ${PARENT_PAGE_ID}`);
    const pages = resp.data.results;
    const newMap = {};
    for (const p of pages) {
      const title = (p.title || "").toLowerCase().trim();
      const matches = title.match(/график\s+([а-яё]+)\s+(\d{4})/);
      if (matches) {
        const monthWord = matches[1];
        const yearStr = matches[2];
        const year = parseInt(yearStr, 10);
        const month = monthNamesRu[monthWord];
        if (year && month) newMap[`${year}-${month}`] = p.id;
      }
    }
    pageMap = newMap;
    console.log('Сформировали карту подстраниц:', pageMap);
  } catch (err) {
    console.error('Ошибка построения карты подстраниц:', err);
  }
}

async function fetchExcelFromConfluence(pageId) {
  try {
    const token = process.env.CONFLUENCE_API_TOKEN;
    const baseUrl = 'https://wiki.sxl.team';
    const attachmentsUrl = `${baseUrl}/rest/api/content/${pageId}/child/attachment`;
    const resp = await axios.get(attachmentsUrl, {
      headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' }
    });
    if (!resp.data || !resp.data.results || resp.data.results.length === 0) {
      throw new Error(`На странице ${pageId} вложений не найдено!`);
    }
    let attachment = resp.data.results.find(a =>
      a.metadata?.mediaType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ) || resp.data.results[0];
    const downloadUrl = attachment._links?.download;
    if (!downloadUrl) throw new Error(`Не найдена ссылка download у вложения на странице ${pageId}`);
    const fileResp = await axios.get(baseUrl + downloadUrl, {
      headers: { 'Authorization': `Bearer ${token}` },
      responseType: 'arraybuffer'
    });
    return Buffer.from(fileResp.data);
  } catch (err) {
    console.error('Ошибка получения Excel из Confluence:', err);
    throw err;
  }
}

function parseScheduleFromBuffer(xlsxBuffer) {
  const workbook = xlsx.read(xlsxBuffer, { type: 'buffer' });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const raw = xlsx.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  let headerRowIndex = -1;
  for (let i = 0; i < raw.length; i++) {
    const firstCell = String(raw[i][0] || "").trim().toLowerCase();
    if (firstCell === "фио") { headerRowIndex = i; break; }
  }
  if (headerRowIndex < 0) throw new Error("В Excel не найдена строка, где первая ячейка = 'ФИО'");
  const dayColumnMap = {};
  const headerRow = raw[headerRowIndex];
  for (let col = 1; col < headerRow.length; col++) {
    const val = String(headerRow[col] || "").trim();
    const dayNum = parseInt(val, 10);
    if (!isNaN(dayNum) && dayNum >= 1 && dayNum <= 31) dayColumnMap[dayNum] = col;
  }
  const schedule = {};
  for (let d = 1; d <= 31; d++) { schedule[d] = { "9-21": [], "10-19": [], "21-9": [] }; }
  let rowIndex = headerRowIndex + 2;
  for (; rowIndex < raw.length; rowIndex++) {
    const row = raw[rowIndex];
    if (!row || row.length === 0) break;
    const fioCell = String(row[0] || "").trim();
    if (!fioCell) break;
    const lowFio = fioCell.toLowerCase();
    if (
      lowFio.startsWith("итого человек") ||
      lowFio.startsWith("итого работает") ||
      lowFio.startsWith("с графиком") ||
      lowFio.startsWith("итого в день") ||
      lowFio === "фио"
    ) break;
    for (const dStr in dayColumnMap) {
      const d = parseInt(dStr, 10);
      const colIndex = dayColumnMap[d];
      const cellVal = String(row[colIndex] || "").trim().toLowerCase().replace(/–/g, '-');
      if (cellVal === "9-21") schedule[d]["9-21"].push(fioCell);
      else if (cellVal === "10-19") schedule[d]["10-19"].push(fioCell);
      else if (cellVal === "21-9") schedule[d]["21-9"].push(fioCell);
    }
  }
  return schedule;
}

async function loadScheduleForMonthYear(year, month) {
  const key = `${year}-${month}`;
  if (!pageMap[key]) {
    console.warn(`Не найден pageId для "${year}-${month}". Возможно, нет подстраницы "График ..."`);
    schedulesByKey[key] = {};
    return;
  }
  const pageId = pageMap[key];
  const buffer = await fetchExcelFromConfluence(pageId);
  const scheduleObj = parseScheduleFromBuffer(buffer);
  schedulesByKey[key] = scheduleObj;
  console.log(`Расписание для ${key} (pageId=${pageId}) успешно загружено.`);
}

async function getScheduleForDate(dt) {
  const y = dt.year, m = dt.month, key = `${y}-${m}`;
  if (!schedulesByKey[key]) {
    console.log(`[getScheduleForDate] Нет расписания для ${key}, пробуем загрузить...`);
    await loadScheduleForMonthYear(y, m);
  }
  const scheduleObj = schedulesByKey[key] || {};
  return scheduleObj[dt.day] || null;
}

// ----------------------------------------------------------------------------------
// 4) Получение дежурного специалиста из Wiki
// ----------------------------------------------------------------------------------
async function fetchDutyEngineer() {
    try {
        const pageId = '3539406'; // ID страницы, где лежит таблица
        const token = process.env.CONFLUENCE_API_TOKEN;

        // Забираем HTML
        const resp = await axios.get(`https://wiki.sxl.team/rest/api/content/${pageId}?expand=body.view`, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'Accept': 'application/json'
            }
        });

        let html = resp.data?.body?.view?.value;
        if (!html) {
            console.log('Не удалось получить HTML из body.view.value');
            return 'Не найдено';
        }

        // Чтобы игнорировать расписание 2024 года, обрезаем HTML до слова "2024"
        const marker = '<span class="expand-control-text conf-macro-render">2024</span>';
        const markerIndex = html.indexOf(marker);
        if (markerIndex !== -1) {
            html = html.slice(0, markerIndex);
        }

        // Теперь парсим строки вида:
        // <tr><td>1</td><td>02.01-08.01</td><td>Иванов И.И.</td></tr>
        // И т.д.

        const rowRegex = /<(?:tr|TR)[^>]*>\s*<td[^>]*>(\d+)<\/td>\s*<td[^>]*>(\d{2}\.\d{2}-\d{2}\.\d{2})<\/td>\s*<td[^>]*>([^<]+)<\/td>/g;
        const schedule = [];
        let match;
        while ((match = rowRegex.exec(html)) !== null) {
            schedule.push({
                index: match[1],      // "1"
                range: match[2],     // "02.01-08.01"
                name: match[3].trim() // "Иванов И.И."
            });
        }

        if (schedule.length === 0) {
            console.log('Не удалось извлечь расписание дежурств из HTML');
            return 'Не найдено';
        }

        // Текущее время (Москва)
        const now = DateTime.now().setZone('Europe/Moscow');

        // Начало текущей недели (понедельник) и конец
        const startOfWeek = now.startOf('week'); // Luxon по умолчанию: startOf('week') = Понедельник
        const endOfWeek = startOfWeek.plus({ days: 6 });
        const currentYear = startOfWeek.year;

        // Проверяем, какой пункт schedule совпадает с этой неделей
        for (const item of schedule) {
            const [startStr, endStr] = item.range.split('-');
            const [startDay, startMonth] = startStr.split('.');
            const [endDay, endMonth] = endStr.split('.');

            const scheduleStart = DateTime.fromObject({
                year: currentYear,
                month: parseInt(startMonth, 10),
                day: parseInt(startDay, 10)
            });
            const scheduleEnd = DateTime.fromObject({
                year: currentYear,
                month: parseInt(endMonth, 10),
                day: parseInt(endDay, 10)
            });

            // Сравниваем
            if (
                scheduleStart.hasSame(startOfWeek, 'day') &&
                scheduleStart.hasSame(startOfWeek, 'month') &&
                scheduleEnd.hasSame(endOfWeek, 'day') &&
                scheduleEnd.hasSame(endOfWeek, 'month')
            ) {
                return item.name;
            }
        }

        return 'Не найдено';
    } catch (error) {
        console.error('Ошибка при запросе к Confluence (дежурный):', error);
        return 'Не найдено';
    }
}

// ----------------------------------------------------------------------------------
// 5) Работа с задачами Jira: получение, сохранение и рассылка
// ----------------------------------------------------------------------------------
function toTextArray(val) {
  if (!val) return [];
  if (Array.isArray(val)) {
    return val
      .map(x => x?.displayName || x?.name || x?.value || (typeof x === "string" ? x : null))
      .filter(Boolean);
  }
  if (typeof val === "object") {
    const one = val.displayName || val.name || val.value;
    return one ? [one] : [];
  }
  return [String(val)];
}

function getReporterDisplay(issue) {
  const r = issue.fields.reporter || issue.fields.creator;
  return r?.displayName || r?.name || "Не указан";
}

function isFastikCase(issue) {
  const it = issue.fields.issuetype?.name;
  const st = issue.fields.status?.name;
  const access = issue.fields.customfield_12206;
  const hasAccess = Array.isArray(access) ? access.length > 0 : Boolean(access);
  return it === "Office" && st === "Waiting for support" && hasAccess;
}

function buildFastikInfo(issue) {
  const rawFor = issue.fields.customfield_12418?.value ?? issue.fields.customfield_12418 ?? "";
  const forLower = String(rawFor).toLowerCase();

  let recipients = [];
  if (forLower.includes("себе")) {
    recipients = [getReporterDisplay(issue)];
  } else if (forLower.includes("своему")) {
    recipients = toTextArray(issue.fields.customfield_12419);
  }

  const accessList = toTextArray(issue.fields.customfield_12206);

  return { fastikFor: String(rawFor || ""), recipients, accessList };
}



async function fetchAndStoreJiraTasks() {
  const parseDepartments = (raw) => {
    return typeof raw === 'string'
      ? raw.split(',').map(dep => dep.trim()).filter(Boolean)
      : [];
  };

  const sxlDepartments = parseDepartments(process.env.JIRA_DEPARTMENTS_SXL);
  const betoneDepartments = parseDepartments(process.env.JIRA_DEPARTMENTS_BETONE);

  await fetchAndStoreTasksFromJira(
    'sxl',
    'https://jira.sxl.team/rest/api/2/search',
    process.env.JIRA_PAT_SXL,
    ...sxlDepartments
  );

  await fetchAndStoreTasksFromJira(
    'betone',
    'https://jira.betone.team/rest/api/2/search',
    process.env.JIRA_PAT_BETONE,
    ...betoneDepartments
  );
}


async function fetchAndStoreTasksFromJira(source, url, pat, ...departments) {
  try {
    console.log(`Fetching tasks from ${source} Jira...`);

    const departmentQuery = departments.map(dep => `"${dep}"`).join(" OR Отдел = ");

    let jql;
    if (source === "sxl") {
      jql = `
        project = SUPPORT AND (
          (issuetype = Infra  AND status = "Open") OR
          (issuetype = Infra  AND status = "Under review") OR
          (issuetype = Office AND status = "Under review") OR
          (issuetype = Office AND status = "Open") OR
          (issuetype = Office AND status = "Waiting for support") OR
          (issuetype = Prod  AND status = "Waiting for Developers approval") OR
          (issuetype = Prod  AND status = "Open") OR
          (Отдел = ${departmentQuery} AND status = "Open") OR
          (Отдел IS EMPTY AND status != "Done")
        )
      `;
    } else {
      jql = `
        project = SUPPORT AND (
          (Отдел = ${departmentQuery} AND status = "Open") OR
          (Отдел IS EMPTY AND status != "Done")
        )
      `;
    }

    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${pat}`, Accept: "application/json" },
      params: {
        jql,
        maxResults: 200,
        fields: [
          "summary",
          "priority",
          "issuetype",
          "status",
          "assignee",
          "reporter",
          "creator",
          "customfield_10500",
          "customfield_10504",
          "customfield_12418",
          "customfield_12419",
          "customfield_12206",
        ].join(","),
      },
    });

    const issues = response.data?.issues || [];
    const fetchedTaskIds = issues.map(issue => `${source}-${issue.key}`);

    // синхронизация: удаляем записи, которых больше нет в выдаче
    if (fetchedTaskIds.length === 0) {
      await new Promise((resolve, reject) => {
        db.run(`DELETE FROM tasks WHERE source = ?`, [source], err => (err ? reject(err) : resolve()));
      });
    } else {
      const placeholders = fetchedTaskIds.map(() => "?").join(",");
      await new Promise((resolve, reject) => {
        db.run(
          `DELETE FROM tasks WHERE source = ? AND id NOT IN (${placeholders})`,
          [source, ...fetchedTaskIds],
          err => (err ? reject(err) : resolve())
        );
      });
    }

    // upsert
    for (const issue of issues) {
      const uniqueId = `${source}-${issue.key}`;

      const reporterObj =
        issue.fields.reporter ||
        issue.fields.creator || { name: "Не указан", displayName: "Не указан" };

      const reporterText = reporterObj.displayName || reporterObj.name;
      const reporterLogin = reporterObj.name || "Не указан";

      const assigneeObj = issue.fields.assignee || { name: "Не указан", displayName: "Не указан" };
      const assigneeText = assigneeObj.displayName || assigneeObj.name;
      const assigneeLogin = assigneeObj.name || "Не указан";

      const status = issue.fields.status?.name || "Не указан";

      // department
      const department =
        (source === "betone" && issue.fields.customfield_10504)
          ? issue.fields.customfield_10504.value
          : (source === "sxl" && issue.fields.customfield_10500)
            ? issue.fields.customfield_10500.value
            : "Не указан";

      // fastik calc
      const fastikNeeded = isFastikCase(issue);
      const fastik = fastikNeeded ? buildFastikInfo(issue) : { fastikFor: "", recipients: [], accessList: [] };

      const task = {
        id: uniqueId,
        title: issue.fields.summary || "",
        priority: issue.fields.priority?.name || "Не указан",
        issueType: issue.fields.issuetype?.name || "Не указан",
        department,
        dateAdded: getMoscowTimestamp(),
        source,
        reporter: reporterText,
        reporterLogin,
        assignee: assigneeText,
        assigneeLogin,
        status,

        fastikNeeded: fastikNeeded ? 1 : 0,
        fastikFor: fastik.fastikFor,
        fastikRecipientsJson: JSON.stringify(fastik.recipients || []),
        fastikAccessJson: JSON.stringify(fastik.accessList || []),
      };

      const existingTask = await new Promise((resolve, reject) => {
        db.get(`SELECT id FROM tasks WHERE id = ?`, [uniqueId], (err, row) => (err ? reject(err) : resolve(row)));
      });

      if (existingTask) {
        db.run(
          `UPDATE tasks SET
            title = ?, priority = ?, issueType = ?, department = ?, source = ?,
            reporter = ?, reporterLogin = ?, assignee = ?, assigneeLogin = ?, status = ?,
            fastikNeeded = ?, fastikFor = ?, fastikRecipientsJson = ?, fastikAccessJson = ?
           WHERE id = ?`,
          [
            task.title, task.priority, task.issueType, task.department, task.source,
            task.reporter, task.reporterLogin, task.assignee, task.assigneeLogin, task.status,
            task.fastikNeeded, task.fastikFor, task.fastikRecipientsJson, task.fastikAccessJson,
            task.id,
          ]
        );
      } else {
        db.run(
          `INSERT OR REPLACE INTO tasks (
            id, title, priority, issueType, department, dateAdded, lastSent,
            source, reporter, reporterLogin, assignee, assigneeLogin, status,
            fastikNeeded, fastikFor, fastikRecipientsJson, fastikAccessJson
          ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            task.id, task.title, task.priority, task.issueType, task.department, task.dateAdded,
            task.source, task.reporter, task.reporterLogin, task.assignee, task.assigneeLogin, task.status,
            task.fastikNeeded, task.fastikFor, task.fastikRecipientsJson, task.fastikAccessJson,
          ]
        );
      }
    }
  } catch (error) {
    console.error(`Error fetching and storing tasks from ${source} Jira:`, error);
  }
}

async function getJiraTaskDetails(source, combinedId) {
  try {
    const realKey = extractRealJiraKey(combinedId);
    const url = `https://jira.${source}.team/rest/api/2/issue/${realKey}?fields=summary,comment,description,attachment,priority,issuetype,status,assignee,reporter,creator`;
    const pat = (source === 'sxl') ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
    console.log(`[getJiraTaskDetails] GET ${url}`);
    const response = await axios.get(url, {
      headers: { 'Authorization': `Bearer ${pat}`, 'Accept': 'application/json' }
    });
    return response.data;
  } catch (error) {
    console.error(`[getJiraTaskDetails] Ошибка GET ${source}-${combinedId}:`, error);
    return null;
  }
}

// Глобальный кэш для хранения message_id по task.id
const messageIdCache = {};

async function sendJiraTasks(ctx) {
  const today = getMoscowTimestamp().split(" ")[0];

  const query = `
    SELECT * FROM tasks
    WHERE
      (
        (department IN ('Техническая поддержка','Не указан')
         AND status NOT IN ('Done','Closed','Resolved')
         AND (lastSent IS NULL OR lastSent < date(?)))
      )
      OR
      (
        (issueType IN ('Infra','Office','Prod')
         AND status NOT IN ('Done','Closed','Resolved')
         AND (lastSent IS NULL OR lastSent < datetime('now','-3 days')))
      )
    ORDER BY CASE
      WHEN department = 'Техническая поддержка' THEN 1
      WHEN department = 'Не указан'            THEN 1
      ELSE 2
    END
  `;

  db.all(query, [today], async (err, rows) => {
    if (err) {
      console.error("Error fetching tasks:", err);
      return;
    }

    for (const task of rows) {
      const keyboard = new InlineKeyboard();

      if (task.department === "Техническая поддержка") {
        keyboard
          .text("Взять в работу", `take_task:${task.id}`)
          .url("Перейти к задаче", getTaskUrl(task.source, task.id))
          .text("⬇ Подробнее", `toggle_description:${task.id}`);
      } else if (["Infra", "Office", "Prod"].includes(task.issueType)) {
        keyboard
          .url("Перейти к задаче", getTaskUrl(task.source, task.id))
          .text("⬇ Подробнее", `toggle_description:${task.id}`);
      }

      let messageText =
        `<b>Задача:</b> ${escapeHtml(task.id)}\n` +
        `<b>Источник:</b> ${escapeHtml(task.source)}\n` +
        `<b>Ссылка:</b> <a href="${escapeHtml(getTaskUrl(task.source, task.id))}">Открыть в Jira</a>\n` +
        `<b>Описание:</b> ${escapeHtml(task.title)}\n` +
        `<b>Приоритет:</b> ${getPriorityEmoji(task.priority)}\n` +
        `<b>Тип задачи:</b> ${escapeHtml(task.issueType)}\n` +
        `<b>Исполнитель:</b> ${escapeHtml(task.assignee)}\n` +
        `<b>Создатель задачи:</b> ${escapeHtml(getHumanReadableName(task.reporterLogin, task.reporter, task.source))}\n` +
        `<b>Статус:</b> ${escapeHtml(task.status)}`;

      // --- FASTIK BLOCK ---
      if (Number(task.fastikNeeded) === 1) {
        let recipients = [];
        let accessList = [];
        try { recipients = JSON.parse(task.fastikRecipientsJson || "[]"); } catch {}
        try { accessList = JSON.parse(task.fastikAccessJson || "[]"); } catch {}

        const who = recipients.join(", ") || "—";
        const what = accessList.join(", ") || "—";
        const whoBy = task.fastikFor ? ` (${escapeHtml(task.fastikFor)})` : "";

        messageText +=
          `\n\n<b>⚡️ Нужно по фастику выдать доступ</b>\n` +
          `<b>Кому:</b> ${escapeHtml(who)}${whoBy}\n` +
          `<b>Доступ:</b> ${escapeHtml(what)}`;
      }

      const sentMessage = await ctx.reply(messageText, {
        reply_markup: keyboard,
        parse_mode: "HTML",
      });

      messageIdCache[task.id] = sentMessage.message_id;

      db.run(`UPDATE tasks SET lastSent = ? WHERE id = ?`, [getMoscowTimestamp(), task.id]);
    }
  });
}


// ----------------------------------------------------------------------------------
// 6) Проверка новых комментариев
// ----------------------------------------------------------------------------------
async function checkForNewComments() {
  try {
    const jql = `project = SUPPORT AND updated >= -7d`;
    const sources = ['sxl', 'betone'];
    const excludedAuthors = Object.values(jiraUserMappings).flatMap(mapping => Object.values(mapping));
    for (const source of sources) {
      const url = `https://jira.${source}.team/rest/api/2/search`;
      const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
      let startAt = 0, total = 0;
      do {
        const response = await axios.get(url, {
          headers: { 'Authorization': `Bearer ${pat}`, 'Accept': 'application/json' },
          params: { jql, maxResults: 50, startAt, fields: 'comment,attachment, assignee,status,reporter,creator,summary,priority,issuetype,customfield_10500,customfield_10504'}
        });
        total = response.data.total;
        const issues = response.data.issues;
        for (const issue of issues) {
          const taskId = `${source}-${issue.key}`;
          let department = 'Не указан';
          if (source === 'sxl') {
            department = issue.fields.customfield_10500?.value || 'Не указан';
          } else {
            department = issue.fields.customfield_10504?.value || 'Не указан';
          }
          const comments = issue.fields.comment?.comments;
          if (!comments || comments.length === 0) continue;
          const lastComment = comments[comments.length - 1];
          const lastCommentId = lastComment.id;
          const author = lastComment.author?.name || 'Не указан';
          const isTechSupportDept = (department === 'Техническая поддержка');
          const isOurComment = excludedAuthors.includes(author);
          if (!isTechSupportDept && !isOurComment) continue;
          db.get('SELECT lastCommentId FROM task_comments WHERE taskId = ?', [taskId], (err, row) => {
            if (err) { console.error('Error fetching last comment from DB:', err); return; }
            if (!row) {
              sendTelegramMessage(taskId, source, issue, lastComment, author, department, isOurComment);
              db.run(`INSERT INTO task_comments (taskId, lastCommentId, assignee) VALUES (?, ?, ?)`,
                [taskId, lastCommentId, issue.fields.assignee?.displayName || 'Не указан']);
            } else if (row.lastCommentId !== lastCommentId) {
              sendTelegramMessage(taskId, source, issue, lastComment, author, department, isOurComment);
              db.run(`UPDATE task_comments SET lastCommentId = ?, assignee = ? WHERE taskId = ?`,
                [lastCommentId, issue.fields.assignee?.displayName || 'Не указан', taskId]);
            }
          });
        }
        startAt += 50;
      } while (startAt < total);
    }
  } catch (error) {
    console.error('Error checking for new comments:', error);
  }
}

// Лимит отправки сообщений
const limiter = new Bottleneck({ minTime: 5000, maxConcurrent: 1 });
const sendMessageWithLimiter = limiter.wrap(async (chatId, text, opts) => {
  await bot.api.sendMessage(chatId, text, opts);
});

const commentCache = {};

/**
 * Функция для получения читаемого имени:
 * Если jiraName (логин) совпадает с маппингом, возвращается ФИО из usernameMappings.
 * Иначе генерируется логин из displayName.
 */
function getHumanReadableName(jiraName, displayName, source) {
  const login = (jiraName || "").trim().toLowerCase();
  if (login) {
    for (const [telegramUser, mapping] of Object.entries(jiraUserMappings)) {
      if ((mapping[source] || "").trim().toLowerCase() === login) {
        return usernameMappings[telegramUser] || displayName;
      }
    }
  }
  const parts = displayName.trim().toLowerCase().split(/\s+/);
  if (parts.length >= 2) {
    const generatedLogin = parts[0][0] + "." + parts[parts.length - 1];
    for (const [telegramUser, mapping] of Object.entries(jiraUserMappings)) {
      if ((mapping[source] || "").trim().toLowerCase() === generatedLogin) {
        return usernameMappings[telegramUser] || displayName;
      }
    }
  }
  return displayName;
}

/**
 * Отправка уведомления о новом комментарии.
 * В уведомлении также отображаются: Исполнитель, Логин исполнителя,
 * Создатель задачи, Логин создателя и Статус.
 */

bot.callbackQuery('refresh_tunnel', async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const currentTunnel = process.env.PUBLIC_BASE_URL || 'Ссылка не установлена';
    await ctx.reply(`🔄 Актуальный URL туннеля:\n${currentTunnel}`);
  } catch (error) {
    console.error('Ошибка при обновлении туннеля:', error);
    await ctx.reply('Не удалось получить актуальный URL туннеля.');
  }
});

async function checkApprovalTasks() {
  try {
    const jql = `project = SUPPORT AND status = "SUPPORT" AND (issuetype = Infra OR issuetype = Prod or issuetype = Office)`;
    const sources = ['sxl'];

    for (const source of sources) {
      const url = `https://jira.${source}.team/rest/api/2/search`;
      const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

      const response = await axios.get(url, {
        headers: { 'Authorization': `Bearer ${pat}`, 'Accept': 'application/json' },
        params: { jql, maxResults: 100, fields: 'assignee,reporter,creator,priority,issuetype,summary,status' }
      });

      for (const issue of response.data.issues) {
        const taskId = `${source}-${issue.key}`;
        const statusName = issue.fields.status?.name || '';
        
        db.get('SELECT lastStatus FROM approval_alerts WHERE taskId = ?', [taskId], async (err, row) => {
          if (err) { console.error('[approval_alerts] select error:', err); return; }
          if (!row || row.lastStatus !== statusName) {
            try {
              await sendApprovalRequest(taskId, source, issue); // ← функция из предыдущего примера
              db.run(
                `INSERT INTO approval_alerts(taskId, lastStatus, lastSentAt)
                 VALUES(?,?,?)
                 ON CONFLICT(taskId) DO UPDATE SET lastStatus=excluded.lastStatus, lastSentAt=excluded.lastSentAt`,
                [taskId, statusName, getMoscowTimestamp()]
              );
            } catch (e) {
              console.error('[approval_alerts] send error:', e);
            }
          }
        });
      }
    }
  } catch (err) {
    console.error('[checkApprovalTasks] error:', err);
  }
}

function getBlessingUrl(source, combinedId) {
  // Работает только для sxl, но можно расширить
  if (source !== 'sxl') return null;
  const realKey = extractRealJiraKey(combinedId);
  return `https://jira.${source}.team/servicedesk/customer/portal/4/${realKey}`;
}



async function sendApprovalRequest(combinedId, source, issue) {
  const keyboard = new InlineKeyboard()
    .url('Перейти к задаче', getTaskUrl(source, combinedId));

  // Добавляем кнопку "Благословить на портале"
  const blessingUrl = getBlessingUrl(source, combinedId);
  if (blessingUrl) {
    keyboard.row().url('🙏 Благословить на портале', blessingUrl);
  }

  const assigneeObj = issue.fields.assignee || null;
  const assigneeText = assigneeObj
    ? getHumanReadableName(assigneeObj.name, assigneeObj.displayName || assigneeObj.name, source)
    : 'Никто';

  const reporterObj = issue.fields.reporter || issue.fields.creator || null;
  const reporterText = reporterObj
    ? getHumanReadableName(reporterObj.name, reporterObj.displayName || reporterObj.name, source)
    : 'Не указан';

  const priority = issue.fields.priority?.name || 'Не указан';
  const taskType = issue.fields.issuetype?.name || 'Не указан';
  const summary  = issue.fields.summary || 'Без названия';
  const statusName = issue.fields.status?.name || 'Не указан';

  const header =
    `<b>Задача:</b> ${combinedId}\n` +
    `<b>Источник:</b> ${source}\n` +
    `<b>Приоритет:</b> ${getPriorityEmoji(priority)}\n` +
    `<b>Тип задачи:</b> ${escapeHtml(taskType)}\n` +
    `<b>Заголовок:</b> ${escapeHtml(summary)}\n` +
    `<b>Исполнитель:</b> ${escapeHtml(assigneeText)}\n` +
    `<b>Создатель задачи:</b> ${escapeHtml(reporterText)}\n` +
    `<b>Статус:</b> ${escapeHtml(statusName)}\n`;

  const text = `😇 <b>Требуется благословить задачу</b>\n\n` + header;

  await sendMessageWithLimiter(process.env.ADMIN_CHAT_ID, text, {
    reply_markup: keyboard,
    parse_mode: 'HTML'
  });
}



async function sendTelegramMessage(combinedId, source, issue, lastComment, authorName, department, isOurComment) {
  const keyboard = new InlineKeyboard().url('Перейти к задаче', getTaskUrl(source, combinedId));

  const assigneeObj = issue.fields.assignee || null;
  const assigneeText = assigneeObj
    ? getHumanReadableName(assigneeObj.name, assigneeObj.displayName || assigneeObj.name, source)
    : 'Никто';

  const reporterObj = issue.fields.reporter || issue.fields.creator || null;
  const reporterText = reporterObj
    ? getHumanReadableName(reporterObj.name, reporterObj.displayName || reporterObj.name, source)
    : 'Не указан';

  const priority = issue.fields.priority?.name || 'Не указан';
  const taskType = issue.fields.issuetype?.name || 'Не указан';
  const summary = issue.fields.summary || 'Без названия';
  const statusName = issue.fields.status?.name || 'Не указан';

  const commentAuthorRaw = lastComment.author?.name || authorName;
  const commentDisplayRaw = lastComment.author?.displayName || authorName;
  const commentAuthor = getHumanReadableName(commentAuthorRaw, commentDisplayRaw, source);

  const rawCommentBody = lastComment.body || '';
  const mentionedAttachments = Array.from(rawCommentBody.matchAll(/!(.+?)\|thumbnail!/gi)).map(m => m[1].trim());

  // Очистка комментария от миниатюр
  let fullCommentHtml = parseCustomMarkdown(rawCommentBody).replace(/!\S+?\|thumbnail!/gi, '');
  const hasThumbnail = mentionedAttachments.length > 0;
  const shortCommentHtml = hasThumbnail
    ? '📎 В комментарии вложение, нажми "Развернуть" для просмотра'
    : safeTruncateHtml(fullCommentHtml, 300);

  const prefix = isOurComment
    ? 'В задаче появился новый комментарий от технической поддержки:\n\n'
    : 'В задаче появился новый комментарий:\n\n';

  const header =
    `<b>Задача:</b> ${combinedId}\n` +
    `<b>Источник:</b> ${source}\n` +
    `<b>Приоритет:</b> ${getPriorityEmoji(priority)}\n` +
    `<b>Тип задачи:</b> ${escapeHtml(taskType)}\n` +
    `<b>Заголовок:</b> ${escapeHtml(summary)}\n` +
    `<b>Исполнитель:</b> ${escapeHtml(assigneeText)}\n` +
    `<b>Создатель задачи:</b> ${escapeHtml(reporterText)}\n` +
    `<b>Автор комментария:</b> ${escapeHtml(commentAuthor)}\n` +
    `<b>Статус:</b> ${escapeHtml(statusName)}\n` +
    `<b>Комментарий:</b>\n`;

  const matchingAttachments = (issue.fields.attachment || []).filter(att =>
    mentionedAttachments.includes(att.filename)
  );

  const cacheKey = `${combinedId}:${lastComment.id}`;
  commentCache[cacheKey] = {
    header: prefix + header,
    shortHtml: shortCommentHtml,
    fullHtml: fullCommentHtml,
    attachments: matchingAttachments,
    source: source
  };

  try {
    db.run(
      `INSERT OR REPLACE INTO comment_cache
       (taskId, commentId, header, shortHtml, fullHtml, attachmentsJson, source)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        combinedId,
        lastComment.id,
        prefix + header,
        shortCommentHtml,
        fullCommentHtml,
        JSON.stringify(matchingAttachments),
        source
      ]
    );
  } catch (err) {
    console.error('[DB] Ошибка при сохранении comment_cache:', err);
  }

  if (hasThumbnail || fullCommentHtml.length > 300) {
    keyboard.text('Развернуть', `expand_comment:${combinedId}:${lastComment.id}`);
  }

  let finalText = commentCache[cacheKey].header + shortCommentHtml;
  finalText = finalText.replace(/<span>/gi, '<tg-spoiler>').replace(/<\/span>/gi, '</tg-spoiler>');

  console.log('[DEBUG] Final message text to send:', finalText);

  sendMessageWithLimiter(process.env.ADMIN_CHAT_ID, finalText, {
    reply_markup: keyboard,
    parse_mode: 'HTML'
  }).catch(e => console.error('Error sending message to Telegram:', e));
}


async function refreshCommentCache(taskId, commentId, source) {
  const issue = await getJiraTaskDetails(source, taskId);
  if (!issue) throw new Error('Не удалось загрузить актуальные данные задачи');

  if (!issue.fields.comment || !issue.fields.comment.comments) {
    throw new Error('В задаче отсутствуют комментарии');
  }

  const lastComment = issue.fields.comment.comments.find(c => c.id === commentId);
  if (!lastComment) throw new Error('Комментарий не найден в задаче');

  const assignee = issue.fields.assignee?.displayName || 'Никто';
  const status = issue.fields.status?.name || '—';
  const priority = issue.fields.priority?.name || 'Не указан';
  const taskType = issue.fields.issuetype?.name || 'Не указан';
  const summary = issue.fields.summary || 'Без названия';

  const header =
    `<b>Задача:</b> ${taskId}\n` +
    `<b>Источник:</b> ${source}\n` +
    `<b>Приоритет:</b> ${getPriorityEmoji(priority)}\n` +
    `<b>Тип задачи:</b> ${escapeHtml(taskType)}\n` +
    `<b>Заголовок:</b> ${escapeHtml(summary)}\n` +
    `<b>Исполнитель:</b> ${escapeHtml(assignee)}\n` +
    `<b>Статус:</b> ${escapeHtml(status)}\n` +
    `<b>Комментарий:</b>\n`;

  const fullHtml = parseCustomMarkdown(lastComment.body || '');
  const attachments = issue.fields.attachment || [];

  const shortHtml = attachments.length > 0
    ? '📎 В комментарии вложение, нажми "Развернуть" для просмотра'
    : safeTruncateHtml(fullHtml, 300);

  const cacheData = {
    header,
    shortHtml,
    fullHtml,
    attachments,
    source,
  };

  commentCache[`${taskId}:${commentId}`] = cacheData;

  db.run(
    `INSERT OR REPLACE INTO comment_cache
    (taskId, commentId, header, shortHtml, fullHtml, attachmentsJson, source)
    VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [taskId, commentId, header, shortHtml, fullHtml, JSON.stringify(attachments), source]
  );

  return cacheData;
}




bot.callbackQuery(/^expand_comment:(.+):(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const combinedId = ctx.match[1];
    const commentId = ctx.match[2];
    const cacheKey = `${combinedId}:${commentId}`;

    let data;

    try {
      data = await refreshCommentCache(combinedId, commentId, combinedId.split('-')[0]);
    } catch (refreshErr) {
      console.error('Ошибка обновления кэша комментария:', refreshErr);
      await ctx.reply('Комментарий был удалён или больше недоступен.');
      return;
    }

    const keyboard = new InlineKeyboard()
      .text('Свернуть', `collapse_comment:${combinedId}:${commentId}`)
      .url('Перейти к задаче', getTaskUrl(data.source, combinedId));

    if (data.attachments.length > 0) {
      let counter = 1;
      const tunnel = process.env.PUBLIC_BASE_URL;

      for (const att of data.attachments) {
        try {
          const fileResp = await axios.get(att.content, {
            responseType: 'arraybuffer',
            headers: {
              'Authorization': `Bearer ${data.source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE}`
            }
          });
          const safeName = att.filename.replace(/[^\w.\-]/g, '_').substring(0, 100);
          const finalName = `${uuidv4()}_${safeName}`;
          const filePath = path.join(ATTACHMENTS_DIR, finalName);
          fs.writeFileSync(filePath, fileResp.data);
          const publicUrl = `${tunnel}/attachments/${finalName}`;
          keyboard.row().url(`Вложение #${counter++}`, publicUrl);
        } catch (errAttach) {
          console.error('Ошибка загрузки вложения:', errAttach);
        }
      }
    }

    await ctx.editMessageText(data.header + data.fullHtml, {
      parse_mode: 'HTML',
      reply_markup: keyboard
    });
  } catch (err) {
    console.error('expand_comment error:', err);
    await ctx.reply('Ошибка при раскрытии комментария.');
  }
});



bot.callbackQuery(/^collapse_comment:(.+):(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const combinedId = ctx.match[1];
    const commentId = ctx.match[2];
    const cacheKey = `${combinedId}:${commentId}`;

    // Обновляем данные из Jira
    let data = await refreshCommentCache(combinedId, commentId, combinedId.split('-')[0]);

    const keyboard = new InlineKeyboard()
      .text('Развернуть', `expand_comment:${combinedId}:${commentId}`)
      .url('Перейти к задаче', getTaskUrl(data.source, combinedId));

    await ctx.editMessageText(data.header + data.shortHtml, {
      parse_mode: 'HTML',
      reply_markup: keyboard
    });
  } catch (err) {
    console.error('collapse_comment error:', err);
    await ctx.reply('Ошибка при сворачивании комментария.');
  }
});




// ----------------------------------------------------------------------------------
// 7) КНОПКА "ВЗЯТЬ В РАБОТУ"
// ----------------------------------------------------------------------------------
async function reassignIssueToRealUser(source, realJiraKey, telegramUsername) {
  try {
    const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
    if (!jiraUsername) {
      console.log(`[reassignIssueToRealUser] Нет маппинга для ${telegramUsername} → Jira (source=${source})`);
      return false;
    }
    const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
    const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${realJiraKey}/assignee`;
    const r = await axios.put(assigneeUrl, { name: jiraUsername }, {
      headers: { 'Authorization': `Bearer ${pat}`, 'Content-Type': 'application/json' }
    });
    if (r.status === 204) {
      console.log(`[reassignIssueToRealUser] Успешно назначили ${realJiraKey} на ${jiraUsername}`);
      return true;
    } else {
      console.warn(`[reassignIssueToRealUser] Статус=${r.status}, не удалось`);
      return false;
    }
  } catch (err) {
    console.error(`[reassignIssueToRealUser] Ошибка:`, err);
    return false;
  }
}

function addInvisibleNoise(text) {
  const invisibleChars = ['\u200B', '\u200C', '\u200D', '\u2060', '\u2061', '\u2062'];
  const randomChar = invisibleChars[Math.floor(Math.random() * invisibleChars.length)];
  return text + randomChar;
}

bot.callbackQuery(/^refresh_task:(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery('🔄 Обновляем данные задачи...');
    const combinedId = ctx.match[1];

    let source;
    let task = await new Promise(resolve => {
      db.get('SELECT * FROM tasks WHERE id = ?', [combinedId], (err, row) => resolve(row));
    });

    if (task) {
      source = task.source;
    } else {
      const txt = ctx.callbackQuery.message?.text || "";
      const match = txt.match(/Источник:\s*([^\n]+)/i);
      source = match ? match[1].trim() : combinedId.split('-')[0];
    }

    const updatedIssue = await getJiraTaskDetails(source, combinedId);
    if (!updatedIssue) {
      return ctx.reply('Не удалось получить данные из Jira.');
    }

    const updatedText =
      `<b>Задача:</b> ${escapeHtml(combinedId)}\n` +
      `<b>Источник:</b> ${escapeHtml(source)}\n` +
      `<b>Ссылка:</b> <a href="${escapeHtml(getTaskUrl(source, combinedId))}">Открыть в Jira</a>\n` +
      `<b>Описание:</b> ${escapeHtml(updatedIssue.fields.summary || task?.title || '')}\n` +
      `<b>Приоритет:</b> ${getPriorityEmoji(updatedIssue.fields.priority?.name || task?.priority || '')}\n` +
      `<b>Тип задачи:</b> ${escapeHtml(updatedIssue.fields.issuetype?.name || task?.issueType || '')}\n` +
      `<b>Исполнитель:</b> ${updatedIssue.fields.assignee 
        ? escapeHtml(getHumanReadableName(
            updatedIssue.fields.assignee.name,
            updatedIssue.fields.assignee.displayName || updatedIssue.fields.assignee.name,
            source
          ))
        : 'Никто'}\n` +
      `<b>Создатель задачи:</b> ${escapeHtml(getHumanReadableName(
        task?.reporterLogin || updatedIssue.fields.reporter?.name,
        task?.reporter || updatedIssue.fields.reporter?.displayName || '',
        source
      ))}\n` +
      `<b>Статус:</b> ${escapeHtml(updatedIssue.fields.status?.name || task?.status || '')}`;

    const keyboard = new InlineKeyboard()
      .text('🔄 Обновить данные', `refresh_task:${combinedId}`)
      .url('Открыть в Jira', getTaskUrl(source, combinedId));

    const finalText = addInvisibleNoise(updatedText); // ← добавляем "невидимый шум"

    await ctx.editMessageText(finalText, {
      parse_mode: 'HTML',
      reply_markup: keyboard
    });

  } catch (err) {
    console.error('refresh_task error:', err);
    await ctx.reply('Ошибка обновления задачи.');
  }
});

bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const combinedId = ctx.match[1];
    const telegramUsername = ctx.from.username;

    db.get('SELECT * FROM tasks WHERE id = ?', [combinedId], async (err, task) => {
      if (err || !task) {
        console.error('Ошибка при получении задачи:', err || 'задача не найдена');
        return ctx.reply('Задача не найдена или ошибка БД.');
      }
      if (task.department !== "Техническая поддержка") {
        return ctx.reply('Эту задачу нельзя взять через бота.');
      }

      const success = await updateJiraTaskStatus(task.source, combinedId, telegramUsername);
      if (!success) {
        return ctx.reply(`Не удалось перевести задачу ${combinedId} в нужный статус.`);
      }

      db.run(
        `INSERT INTO user_actions (username, taskId, action, timestamp)
         VALUES (?, ?, ?, ?)`,
        [telegramUsername, combinedId, 'take_task', getMoscowTimestamp()]
      );

      const displayName = usernameMappings[telegramUsername] || telegramUsername;
      await ctx.reply(`✅ Задачу ${combinedId} взял в работу: ${displayName}.`);

      const updatedIssue = await getJiraTaskDetails(task.source, combinedId);
      if (!updatedIssue) {
        return console.error('Не удалось получить данные из Jira.');
      }

      const newMessageText = 
        `<b>Задача:</b> ${escapeHtml(combinedId)}\n` +
        `<b>Источник:</b> ${escapeHtml(task.source)}\n` +
        `<b>Ссылка:</b> <a href="${escapeHtml(getTaskUrl(task.source, task.id))}">Открыть в Jira</a>\n` +
        `<b>Описание:</b> ${escapeHtml(updatedIssue.fields.summary || task.title)}\n` +
        `<b>Приоритет:</b> ${getPriorityEmoji(updatedIssue.fields.priority?.name || task.priority)}\n` +
        `<b>Тип задачи:</b> ${escapeHtml(updatedIssue.fields.issuetype?.name || task.issueType)}\n` +
        `<b>Исполнитель:</b> ${updatedIssue.fields.assignee 
          ? escapeHtml(getHumanReadableName(
              updatedIssue.fields.assignee.name,
              updatedIssue.fields.assignee.displayName || updatedIssue.fields.assignee.name,
              task.source
            ))
          : 'Никто'}\n` +
        `<b>Создатель задачи:</b> ${escapeHtml(getHumanReadableName(task.reporterLogin, task.reporter, task.source))}\n` +
        `<b>Статус:</b> ${escapeHtml(updatedIssue.fields.status?.name || task.status)}`;

      // Добавляем кнопку обновления данных и перехода к задаче
      const keyboard = new InlineKeyboard()
        .text('🔄 Обновить данные', `refresh_task:${combinedId}`)
        .url('Открыть в Jira', getTaskUrl(task.source, task.id));

      // Обновляем исходное сообщение
      const messageId = messageIdCache[combinedId];
      if (messageId) {
        await bot.api.editMessageText(
          process.env.ADMIN_CHAT_ID,
          messageId,
          newMessageText,
          { parse_mode: 'HTML', reply_markup: keyboard }
        );
      }

      // Переназначаем исполнителя через 30 секунд
      setTimeout(async () => {
        const realKey = extractRealJiraKey(combinedId);
        await reassignIssueToRealUser(task.source, realKey, telegramUsername);
      }, 30000);
    });
  } catch (error) {
    console.error('Ошибка в take_task:', error);
    await ctx.reply('Произошла ошибка.');
  }
});




async function updateJiraTaskStatus(source, combinedId, telegramUsername) {
  try {
    const realKey = extractRealJiraKey(combinedId);
    let transitionId = source === 'sxl' ? '221' : '201';
    const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
    if (!jiraUsername) {
      console.error(`No Jira username for telegram user: ${telegramUsername}`);
      return false;
    }
    const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
    const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${realKey}/assignee`;
    const r1 = await axios.put(assigneeUrl, { name: jiraUsername }, {
      headers: { 'Authorization': `Bearer ${pat}`, 'Content-Type': 'application/json' }
    });
    if (r1.status !== 204) {
      console.error('Assignee error:', r1.status);
      return false;
    }
    const transitionUrl = `https://jira.${source}.team/rest/api/2/issue/${realKey}/transitions`;
    const r2 = await axios.post(transitionUrl, { transition: { id: transitionId } }, {
      headers: { 'Authorization': `Bearer ${pat}`, 'Content-Type': 'application/json' }
    });
    return (r2.status === 204);
  } catch (error) {
    console.error(`Error updating Jira task:`, error);
    return false;
  }
}

// ----------------------------------------------------------------------------------
// 8) Функции парсинга и форматирования описания (Markdown → HTML)
// ----------------------------------------------------------------------------------
function escapeHtml(text) {
  if (!text) return '';
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function convertCodeBlocks(input) {
  let output = input;
  output = output.replace(/\{code:([\w\-]+)\}([\s\S]*?)\{code\}/g, (match, lang, code) => {
    return `<pre><code class="language-${lang}">${escapeHtml(code.trim())}</code></pre>`;
  });
  output = output.replace(/\{code\}([\s\S]*?)\{code\}/g, (match, code) => {
    return `<pre><code>${escapeHtml(code.trim())}</code></pre>`;
  });
  return output;
}

function convertNoformatBlocks(text) {
  return text.replace(/\{noformat\}([\s\S]*?)\{noformat\}/g, (match, content) => {
    return `<pre>${escapeHtml(content.trim())}</pre>`;
  });
}

function convertSquareBracketLinks(text) {
  return text.replace(/\[([^\|\]]+)\|([^\]]+)\]/g, (match, linkText, linkUrl) => {
    const safeText = escapeHtml(linkText.trim());
    const safeUrl = escapeHtml(linkUrl.trim());
    return `<a href="${safeUrl}">${safeText}</a>`;
  });
}

function safeTruncateHtml(html, maxLength) {
  if (html.length <= maxLength) return html;
  let truncated = html.slice(0, maxLength);
  const lastOpenIndex = truncated.lastIndexOf('<');
  const lastCloseIndex = truncated.lastIndexOf('>');
  if (lastOpenIndex > lastCloseIndex) {
    truncated = truncated.slice(0, lastOpenIndex);
  }
  return truncated + '...';
}

function convertHashLinesToNumbered(text) {
  let lines = text.split('\n');
  let result = [];
  let counter = 1;
  console.log('[DEBUG] Lines before processing:');
  for (let i = 0; i < lines.length; i++) {
    console.log(i, JSON.stringify(lines[i]), lines[i].split('').map(c => c.charCodeAt(0)));
  }
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i].replace(/\u00A0/g, ' ');
    let trimmed = line.trim();
    if (trimmed === '#' || trimmed === '#') {
      let nextIndex = i + 1;
      let foundText = null;
      while (nextIndex < lines.length) {
        let candidate = lines[nextIndex].replace(/\u00A0/g, ' ').trim();
        if (candidate) { foundText = candidate; break; }
        nextIndex++;
      }
      if (foundText) { result.push(`${counter++}) ${foundText}`); i = nextIndex; }
      else { result.push(`${counter++})`); }
    } else if (trimmed.startsWith('# ')) {
      const content = trimmed.slice(2);
      result.push(`${counter++}) ${content}`);
    } else { result.push(line); }
  }
  return result.join('\n');
}

function formatTables(text) {
  return text.replace(/\|(.+?)\|/g, match => {
    return `<pre>${escapeHtml(match.trim())}</pre>`;
  });
}

function parseCustomMarkdown(text) {
  if (!text) return '';
  text = convertNoformatBlocks(text);
  text = convertCodeBlocks(text);
  text = formatTables(text);
  text = convertSquareBracketLinks(text);
  text = convertHashLinesToNumbered(text);
  text = text
    .replace(/\*(.*?)\*/g, '<b>$1</b>')
    .replace(/_(.*?)_/g, '<i>$1</i>')
    .replace(/\+(.*?)\+/g, '<u>$1</u>')
    .replace(/~~(.*?)~~/g, '<s>$1</s>')
    .replace(/(^|\s)`([^`]+)`(\s|$)/g, '$1<code>$2</code>$3')
    .replace(/^\-\s(.*)/gm, '• $1')
    .replace(/^\*\s(.*)/gm, '• $1')
    .replace(/^\d+\.\s(.*)/gm, '🔹 $1')
    .replace(/\n{3,}/g, '\n\n');
  return text;
}

function formatDescriptionAsHtml(rawDescription) {
  return parseCustomMarkdown(rawDescription || '');
}

// ----------------------------------------------------------------------------------
// 8) Callback "toggle_description" для переключения описания задачи
// ----------------------------------------------------------------------------------
bot.callbackQuery(/^toggle_description:(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const combinedId = ctx.match[1];
    let rowFromDb = await new Promise(resolve => {
      db.get('SELECT * FROM tasks WHERE id = ?', [combinedId], (err, row) => resolve(row));
    });
    let source = rowFromDb?.source;
    if (!source) {
      const txt = ctx.callbackQuery.message?.text || "";
      const match = txt.match(/Источник:\s*([^\n]+)/i);
      source = match ? match[1].trim() : combinedId.split('-')[0];
    }
    const issue = await getJiraTaskDetails(source, combinedId);
    if (!issue) {
      return ctx.reply('Не удалось получить данные задачи из Jira.');
    }
    const summary = issue.fields.summary || 'Без названия';
    const description = issue.fields.description || 'Нет описания';
    const statusName = issue.fields.status?.name || '—';
    const priority = issue.fields.priority?.name || 'None';
    const taskType = issue.fields.issuetype?.name || '—';
    const assigneeObj = issue.fields.assignee || null;
    const priorityEmoji = getPriorityEmoji(priority);
    let assigneeText = 'Никто';
    if (assigneeObj) {
      assigneeText = getHumanReadableName(assigneeObj.name, assigneeObj.displayName || assigneeObj.name, source);
    }
    const reporterObj = issue.fields.reporter || null;
    let reporterText = 'Не указан';
    if (reporterObj) {
      reporterText = getHumanReadableName(reporterObj.name, reporterObj.displayName || reporterObj.name, source);
    }
    const currentText = ctx.callbackQuery.message?.text.trimEnd() || "";
    const isExpanded = currentText.endsWith("...");
    const keyboard = new InlineKeyboard();
    if ((rowFromDb?.department === "Техническая поддержка") && (statusName === "Open")) {
      keyboard.text('Взять в работу', `take_task:${combinedId}`);
    }
    keyboard
      .text(isExpanded ? 'Подробнее' : 'Скрыть', `toggle_description:${combinedId}`)
      .url('Открыть в Jira', `https://jira.${source}.team/browse/${extractRealJiraKey(combinedId)}`);
    
    if (!isExpanded) {
      const safeDesc = formatDescriptionAsHtml(description);
      // --- Новая логика для вставки вложений ---
      if (issue.fields.attachment && Array.isArray(issue.fields.attachment) && issue.fields.attachment.length > 0) {
        let counter = 1;
        for (const att of issue.fields.attachment) {
          try {
            const fileResp = await axios.get(att.content, {
              responseType: 'arraybuffer',
              headers: {
                'Authorization': `Bearer ${source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE}`
              }
            });
            const originalFilename = att.filename.replace(/[^\w.\-]/g, '_').substring(0, 100);
            const finalName = `${uuidv4()}_${originalFilename}`;
            const filePath = path.join(ATTACHMENTS_DIR, finalName);
            fs.writeFileSync(filePath, fileResp.data);
            const publicUrl = `${process.env.PUBLIC_BASE_URL}/attachments/${finalName}`;
            // Добавляем новую строку с кнопкой для вложения в клавиатуру
            keyboard.row().url(`Вложение #${counter++}`, publicUrl);
          } catch (errAttach) {
            console.error('Ошибка при скачивании вложения:', errAttach);
          }
        }
      }
      // --- Конец новой логики ---
      
      await ctx.editMessageText(
        `<b>Задача:</b> ${combinedId}\n` +
        `<b>Источник:</b> ${source}\n` +
        `<b>Приоритет:</b> ${priorityEmoji}\n` +
        `<b>Тип задачи:</b> ${escapeHtml(taskType)}\n` +
        `<b>Заголовок:</b> ${escapeHtml(summary)}\n` +
        `<b>Исполнитель:</b> ${escapeHtml(assigneeText)}\n` +
        `<b>Создатель задачи:</b> ${escapeHtml(reporterText)}\n` +
        `<b>Статус:</b> ${escapeHtml(statusName)}\n\n` +
        `<b>Описание:</b>\n${safeDesc}\n\n...`,
        { parse_mode: 'HTML', reply_markup: keyboard }
      );
    } else {
      await ctx.editMessageText(
        `<b>Задача:</b> ${combinedId}\n` +
        `<b>Источник:</b> ${source}\n` +
        `<b>Ссылка:</b> <a href="https://jira.${source}.team/browse/${extractRealJiraKey(combinedId)}">Открыть в Jira</a>\n` +
        `<b>Заголовок:</b> ${escapeHtml(summary)}\n` +
        `<b>Приоритет:</b> ${priorityEmoji}\n` +
        `<b>Тип задачи:</b> ${taskType}\n` +
        `<b>Исполнитель:</b> ${escapeHtml(assigneeText)}\n` +
        `<b>Создатель задачи:</b> ${escapeHtml(reporterText)}\n` +
        `<b>Статус:</b> ${escapeHtml(statusName)}\n`,
        { parse_mode: 'HTML', reply_markup: keyboard }
      );
    }
  } catch (err) {
    console.error('toggle_description error:', err);
    await ctx.reply('Ошибка при обработке toggle_description');
  }
});



// ----------------------------------------------------------------------------------
// 7) КНОПКА "ВЗЯТЬ В РАБОТУ"
// ----------------------------------------------------------------------------------
async function reassignIssueToRealUser(source, realJiraKey, telegramUsername) {
  try {
    const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
    if (!jiraUsername) {
      console.log(`[reassignIssueToRealUser] Нет маппинга для ${telegramUsername} → Jira (source=${source})`);
      return false;
    }
    const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
    const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${realJiraKey}/assignee`;
    const r = await axios.put(assigneeUrl, { name: jiraUsername }, {
      headers: { 'Authorization': `Bearer ${pat}`, 'Content-Type': 'application/json' }
    });
    if (r.status === 204) {
      console.log(`[reassignIssueToRealUser] Успешно назначили ${realJiraKey} на ${jiraUsername}`);
      return true;
    } else {
      console.warn(`[reassignIssueToRealUser] Статус=${r.status}, не удалось`);
      return false;
    }
  } catch (err) {
    console.error(`[reassignIssueToRealUser] Ошибка:`, err);
    return false;
  }
}

bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const combinedId = ctx.match[1];
    const telegramUsername = ctx.from.username;

    db.get('SELECT * FROM tasks WHERE id = ?', [combinedId], async (err, task) => {
      if (err) {
        console.error('Ошибка при получении задачи:', err);
        return ctx.reply('Произошла ошибка при получении задачи.');
      }
      if (!task) return ctx.reply('Задача не найдена в БД.');
      if (task.department !== "Техническая поддержка") {
        return ctx.reply('Эта задача не для ТП; нельзя взять в работу через бота.');
      }

      let success = false;
      try {
        success = await updateJiraTaskStatus(task.source, combinedId, telegramUsername);
      } catch (errUpd) {
        console.error('Ошибка updateJiraTaskStatus:', errUpd);
      }

      if (success) {
        db.run(
          `INSERT INTO user_actions (username, taskId, action, timestamp)
           VALUES (?, ?, ?, ?)`,
          [telegramUsername, combinedId, 'take_task', getMoscowTimestamp()]
        );
        const displayName = usernameMappings[telegramUsername] || telegramUsername;
        await ctx.reply(`OK, задачу ${escapeHtml(combinedId)} взял в работу: ${escapeHtml(displayName)}.`);

        // Получаем обновлённые данные задачи из Jira
        const updatedIssue = await getJiraTaskDetails(task.source, combinedId);
        if (!updatedIssue) {
          console.error('Не удалось получить обновленные данные из Jira.');
          return;
        }
        // Формируем новый текст сообщения с использованием escapeHtml для динамических данных,
        // а также вызываем getHumanReadableName для отображения создателя задачи.
        const newMessageText =
          `<b>Задача:</b> ${escapeHtml(combinedId)}\n` +
          `<b>Источник:</b> ${escapeHtml(task.source)}\n` +
          `<b>Ссылка:</b> <a href="${escapeHtml(getTaskUrl(task.source, task.id))}">${escapeHtml(getTaskUrl(task.source, task.id))}</a>\n` +
          `<b>Описание:</b> ${escapeHtml(updatedIssue.fields.summary || task.title)}\n` +
          `<b>Приоритет:</b> ${getPriorityEmoji(updatedIssue.fields.priority?.name || task.priority)}\n` +
          `<b>Тип задачи:</b> ${escapeHtml(updatedIssue.fields.issuetype?.name || task.issueType)}\n` +
          `<b>Исполнитель:</b> ${updatedIssue.fields.assignee 
             ? escapeHtml(getHumanReadableName(
                   updatedIssue.fields.assignee.name,
                   updatedIssue.fields.assignee.displayName || updatedIssue.fields.assignee.name,
                   task.source
                 ))
             : 'Никто'}\n` +
          `<b>Создатель задачи:</b> ${escapeHtml(getHumanReadableName(task.reporter, task.reporter, task.source))}\n` +
          `<b>Статус:</b> ${escapeHtml(updatedIssue.fields.status?.name || task.status)}`;

        // Если в кэше есть message_id исходного сообщения, редактируем его
        const messageId = messageIdCache[combinedId];
        if (messageId) {
          try {
            await bot.api.editMessageText(process.env.ADMIN_CHAT_ID, messageId, newMessageText, { parse_mode: 'HTML' });
          } catch (errEdit) {
            console.error('Ошибка при редактировании сообщения:', errEdit);
          }
        }

        // Дополнительно: через 30 секунд вызываем reassignIssueToRealUser (если необходимо)
        setTimeout(async () => {
          const realKey = extractRealJiraKey(combinedId);
          const reassignOk = await reassignIssueToRealUser(task.source, realKey, telegramUsername);
          if (reassignOk) {
            console.log(`Задача ${combinedId} (реально) переназначена на ${telegramUsername}`);
          }
        }, 30000);
      } else {
        await ctx.reply(`Не удалось перевести задачу ${escapeHtml(combinedId)} в нужный статус (updateJiraTaskStatus failed)`);
      }
    });
  } catch (error) {
    console.error('Ошибка в take_task:', error);
    await ctx.reply('Произошла ошибка.');
  }
});



async function updateJiraTaskStatus(source, combinedId, telegramUsername) {
  try {
    const realKey = extractRealJiraKey(combinedId);
    let transitionId = source === 'sxl' ? '221' : '201';
    const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
    if (!jiraUsername) {
      console.error(`No Jira username for telegram user: ${telegramUsername}`);
      return false;
    }
    const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
    const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${realKey}/assignee`;
    const r1 = await axios.put(assigneeUrl, { name: jiraUsername }, {
      headers: { 'Authorization': `Bearer ${pat}`, 'Content-Type': 'application/json' }
    });
    if (r1.status !== 204) {
      console.error('Assignee error:', r1.status);
      return false;
    }
    const transitionUrl = `https://jira.${source}.team/rest/api/2/issue/${realKey}/transitions`;
    const r2 = await axios.post(transitionUrl, { transition: { id: transitionId } }, {
      headers: { 'Authorization': `Bearer ${pat}`, 'Content-Type': 'application/json' }
    });
    return (r2.status === 204);
  } catch (error) {
    console.error(`Error updating Jira task:`, error);
    return false;
  }
}


// ----------------------------------------------------------------------------------
// 10) Cron-задачи и дополнительные команды
// ----------------------------------------------------------------------------------
async function getDayMessageText() {
  const now = getMoscowDateTime();
  const daySchedule = await getScheduleForDate(now);
  const engineer = await fetchDutyEngineer();
  if (!daySchedule) {
    return `Расписание на сегодня (${now.toFormat("dd.MM.yyyy")}) не найдено.\n<b>Дежурный специалист DevOPS:</b> ${engineer}`;
  }
  const arr9_21 = daySchedule["9-21"] || [];
  const arr10_19 = daySchedule["10-19"] || [];
  const arr21_9 = daySchedule["21-9"] || [];
  return `🔔 <b>Расписание на сегодня, ${now.toFormat("dd.MM.yyyy")} (10:00)</b>\n` +
         `\n<b>Дневная (9-21):</b> ${arr9_21.join(", ") || "—"}\n` +
         `<b>Дневная 5/2 (10-19):</b> ${arr10_19.join(", ") || "—"}\n` +
         `<b>Сегодня в ночь (21-9):</b> ${arr21_9.join(", ") || "—"}\n` +
         `\n<b>Дежурный специалист DevOPS:</b> ${engineer}`;
}

async function getNightMessageText() {
  const now = getMoscowDateTime();
  const todaySchedule = await getScheduleForDate(now) || {};
  const tomorrow = now.plus({ days: 1 });
  const tomorrowSchedule = await getScheduleForDate(tomorrow) || {};
  const arr21_9_today = todaySchedule["21-9"] || [];
  const arr9_21_tomorrow = tomorrowSchedule["9-21"] || [];
  const arr10_19_tomorrow = tomorrowSchedule["10-19"] || [];
  const engineer = await fetchDutyEngineer();
  return `🌙 <b>Расписание вечер, ${now.toFormat("dd.MM.yyyy")} (21:00)</b>\n` +
         `\n<b>Сегодня в ночь (21-9):</b> ${arr21_9_today.join(", ") || "—"}\n` +
         `<b>Завтра утро (9-21):</b> ${arr9_21_tomorrow.join(", ") || "—"}\n` +
         `<b>Завтра 5/2 (10-19):</b> ${arr10_19_tomorrow.join(", ") || "—"}\n` +
         `\n<b>Дежурный специалист DevOPS:</b> ${engineer}`;
}

cron.schedule('* * * * *', async () => {
  try {
    console.log('[CRON] Обновление задач из Jira...');
    await fetchAndStoreJiraTasks();
    const ctx = { reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts) };
    await sendJiraTasks(ctx);
  } catch (err) {
    console.error('Ошибка в CRON fetchAndStoreJiraTasks/sendJiraTasks:', err);
  }
}, { timezone: 'Europe/Moscow' });

cron.schedule('*/5 * * * *', async () => {
  try {
    console.log('[CRON] Проверка новых комментариев...');
    await checkForNewComments();
  } catch (err) {
    console.error('Ошибка в CRON checkForNewComments:', err);
  }
}, { timezone: 'Europe/Moscow' });

cron.schedule('0 10 * * *', async () => {
  try {
    const text = await getDayMessageText();
    await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, { parse_mode: 'HTML' });
  } catch (err) {
    console.error('[CRON 10:00] Ошибка:', err);
  }
}, { timezone: 'Europe/Moscow' });

cron.schedule('0 21 * * *', async () => {
  try {
    const text = await getNightMessageText();
    await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, { parse_mode: 'HTML' });
  } catch (err) {
    console.error('[CRON 21:00] Ошибка:', err);
  }
}, { timezone: 'Europe/Moscow' });

cron.schedule('* * * * *', async () => {
  try {
    console.log('[CRON] Проверка задач на благословение...');
    await checkApprovalTasks();
  } catch (err) {
    console.error('Ошибка в CRON checkApprovalTasks:', err);
  }
}, { timezone: 'Europe/Moscow' });


cron.schedule('0 11 * * *', async () => {
  try {
    const now = getMoscowDateTime();
    const daysInMonth = now.daysInMonth;
    const today = now.day;
    if (today === daysInMonth) {
      const nextMonth = now.plus({ months: 1 });
      await bot.api.sendMessage(process.env.ADMIN_CHAT_ID,
        `Сегодня ${now.toFormat("dd.MM.yyyy")} — последний день месяца.\nПодгружаем расписание на следующий месяц (${nextMonth.toFormat("LLLL yyyy")})...`
      );
      await buildPageMapForSchedules();
      await loadScheduleForMonthYear(nextMonth.year, nextMonth.month);
      await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, '✅ Готово, расписание следующего месяца загружено.');
    }
  } catch (err) {
    console.error('[CRON LAST DAY] Ошибка:', err);
    bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Ошибка при загрузке расписания на следующий месяц');
  }
}, { timezone: 'Europe/Moscow' });

// ----------------------------------------------------------------------------------
// 11) Дополнительные команды: /test_day, /test_night, /duty, /forcestart
// ----------------------------------------------------------------------------------
bot.command('test_day', async (ctx) => {
  try {
    const text = await getDayMessageText();
    await ctx.reply(text, { parse_mode: 'HTML' });
  } catch (err) {
    console.error('Ошибка /test_day:', err);
    await ctx.reply('Ошибка при формировании дневного сообщения');
  }
});

bot.command('test_night', async (ctx) => {
  try {
    const text = await getNightMessageText();
    await ctx.reply(text, { parse_mode: 'HTML' });
  } catch (err) {
    console.error('Ошибка /test_night:', err);
    await ctx.reply('Ошибка при формировании вечернего сообщения');
  }
});

bot.command('duty', async (ctx) => {
  try {
    const engineer = await fetchDutyEngineer();
    await ctx.reply(`Дежурный: ${engineer}`);
  } catch (err) {
    console.error('Ошибка duty:', err);
    await ctx.reply('Произошла ошибка при запросе дежурного.');
  }
});

bot.command('forcestart', async (ctx) => {
  await initializeBotTasks();
  await ctx.reply('♻️ Все задачи были перезапущены (и расписание перечитано).');
});


// ----------------------------------------------------------------------------------
// DB MIGRATION: добавить fastik-поля в уже существующую таблицу tasks
// ----------------------------------------------------------------------------------
function runAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) return reject(err);
      resolve(this);
    });
  });
}

function allAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

async function ensureTasksColumns() {
  // читаем текущие колонки
  const cols = await allAsync(`PRAGMA table_info(tasks)`);
  const colNames = new Set(cols.map(c => String(c.name).toLowerCase()));

  const toAdd = [
    { name: "fastikNeeded", type: "INTEGER", def: "0" },
    { name: "fastikFor", type: "TEXT", def: null },
    { name: "fastikRecipientsJson", type: "TEXT", def: null },
    { name: "fastikAccessJson", type: "TEXT", def: null },
  ];

  for (const c of toAdd) {
    if (colNames.has(c.name.toLowerCase())) continue;

    const defSql = c.def === null ? "" : ` DEFAULT ${c.def}`;
    const sql = `ALTER TABLE tasks ADD COLUMN ${c.name} ${c.type}${defSql}`;
    console.log("[DB MIGRATION]", sql);
    await runAsync(sql);
  }

  console.log("[DB MIGRATION] tasks columns OK");
}

// ----------------------------------------------------------------------------------
// 12) Инициализация при старте
// ----------------------------------------------------------------------------------
async function initializeBotTasks() {
  try {
    console.log('[BOT INIT] Запуск задач...');
    await ensureTasksColumns(); // ✅ добавили миграцию сюда
    await buildPageMapForSchedules();
    const now = getMoscowDateTime();
    await loadScheduleForMonthYear(now.year, now.month);
    await fetchAndStoreJiraTasks();
    const ctx = { reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts) };
    await sendJiraTasks(ctx);
    await checkForNewComments();
    db.all('SELECT taskId FROM task_comments', [], (err, rows) => {
      if (err) console.error('Error fetching task_comments:', err);
      else console.log(`Total task_comments in DB: ${rows.length}`);
    });
    console.log('[BOT INIT] Всё готово.');
  } catch (err) {
    console.error('[BOT INIT] Ошибка:', err);
  }
}

bot.command('start', async (ctx) => {
  await ctx.reply('✅ Бот запущен. Все задачи работают. (/forcestart для повторного запуска)');
});

bot.start({
  onStart: (botInfo) => {
    console.log(`✅ Bot ${botInfo.username} is running`);
    initializeBotTasks();
  }
});