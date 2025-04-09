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
// 1) ИНИЦИАЛИЗАЦИЯ БОТА, БАЗЫ, ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ----------------------------------------------------------------------------------

const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');

// Получение московского времени "yyyy-MM-dd HH:mm:ss"
function getMoscowTimestamp() {
    return DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd HH:mm:ss');
}

// Получение Luxon‐DateTime (Москва)
function getMoscowDateTime() {
    return DateTime.now().setZone('Europe/Moscow');
}

// Создаём нужные таблицы в SQLite (если ещё не созданы)
db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        title TEXT,
        priority TEXT,
        department TEXT,
        issueType TEXT,
        dateAdded DATETIME,
        lastSent DATETIME,
        source TEXT
    )`);

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
});

// Приоритет Jira → эмодзи
function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

// Функция, отделяющая "sxl-" или "betone-" от реального ключа
function extractRealJiraKey(fullId) {
    if (fullId.startsWith('sxl-') || fullId.startsWith('betone-')) {
        const parts = fullId.split('-');
        parts.shift();
        return parts.join('-');
    }
    return fullId;
}

// Генерация URL для Jira
function getTaskUrl(source, combinedId) {
    const realKey = extractRealJiraKey(combinedId);
    return `https://jira.${source}.team/browse/${realKey}`;
}

// Маппинг Telegram username → ФИО
const usernameMappings = {
    "lipchinski": "Дмитрий Селиванов",
    "pr0spal": "Евгений Шушков",
    "fdhsudgjdgkdfg": "Даниил Маслов",
    "EuroKaufman": "Даниил Баратов",
    "Nikolay_Gonchar": "Николай Гончар",
    "KIRILlKxX": "Кирилл Атанизяов",
    "marysh353": "Даниил Марышев"
};

// Маппинг Telegram username → Jira username
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
// 2) EXPRESS (раздача вложений Jira) и очистка
// ----------------------------------------------------------------------------------

const app = express();
const ATTACHMENTS_DIR = path.join(__dirname, 'attachments');

if (!fs.existsSync(ATTACHMENTS_DIR)) {
    fs.mkdirSync(ATTACHMENTS_DIR);
}
app.use('/attachments', express.static(ATTACHMENTS_DIR));

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Express server listening on port ${PORT}`);
});

// Крон на 3:00 — удалять файлы из attachments старше суток
cron.schedule('0 3 * * *', () => {
    console.log('[CRON] Удаляем старые файлы из attachments...');
    const now = Date.now();
    const cutoff = now - 24 * 60 * 60 * 1000;

    fs.readdir(ATTACHMENTS_DIR, (err, files) => {
        if (err) {
            console.error('Ошибка чтения папки attachments:', err);
            return;
        }
        files.forEach(file => {
            const filePath = path.join(ATTACHMENTS_DIR, file);
            fs.stat(filePath, (statErr, stats) => {
                if (statErr) {
                    console.error('Ошибка fs.stat:', statErr);
                    return;
                }
                if (stats.mtimeMs < cutoff) {
                    fs.unlink(filePath, delErr => {
                        if (delErr) {
                            console.error('Ошибка удаления файла:', delErr);
                        } else {
                            console.log(`Файл ${file} удалён (старше суток)`);
                        }
                    });
                }
            });
        });
    });
}, {
    timezone: 'Europe/Moscow'
});

// ----------------------------------------------------------------------------------
// 3) СБОР ИНФОРМАЦИИ ПО ВСЕМ ПОДСТРАНИЦАМ ("График ... 2025") И ПАРСИНГ EXCEL
// ----------------------------------------------------------------------------------

// Словарь для сопоставления русских названий месяцев → номер (1..12)
const monthNamesRu = {
    'январь': 1,
    'февраль': 2,
    'март': 3,
    'апрель': 4,
    'май': 5,
    'июнь': 6,
    'июль': 7,
    'август': 8,
    'сентябрь': 9,
    'октябрь': 10,
    'ноябрь': 11,
    'декабрь': 12
};

// Родительская страница, где лежат подстраницы вида «График январь 2025»
const PARENT_PAGE_ID = '55414233'; 

// Здесь храним сопоставление "2025-4" → childPageId (например, 96732191)
let pageMap = {}; 

// Здесь храним готовые расписания: schedulesByKey["2025-4"] = {...}
const schedulesByKey = {};

/**
 * 3.1) Считать список всех child pages родительской (PARENT_PAGE_ID),
 *      извлечь их ID и заголовок. Из заголовка типа «График январь 2025»
 *      парсим (месяц, год) и формируем "YYYY-M" → pageId.
 */
async function buildPageMapForSchedules() {
    const baseUrl = 'https://wiki.sxl.team';
    const confluenceToken = process.env.CONFLUENCE_API_TOKEN;

    const url = `${baseUrl}/rest/api/content/${PARENT_PAGE_ID}/child/page?limit=200`;
    const resp = await axios.get(url, {
        headers: {
            'Authorization': `Bearer ${confluenceToken}`,
            'Accept': 'application/json'
        }
    });

    if (!resp.data || !resp.data.results) {
        throw new Error(`Не удалось прочитать дочерние страницы для ${PARENT_PAGE_ID}`);
    }

    const pages = resp.data.results; // массив
    const newMap = {};

    for (const p of pages) {
        const title = (p.title || "").toLowerCase().trim(); // "график апрель 2025"
        const matches = title.match(/график\s+([а-яё]+)\s+(\d{4})/);
        if (matches) {
            const monthWord = matches[1]; // "апрель"
            const yearStr = matches[2];    // "2025"

            const year = parseInt(yearStr, 10);
            const month = monthNamesRu[monthWord]; // 4

            if (year && month) {
                // Ключ вида "2025-4"
                const key = `${year}-${month}`;
                newMap[key] = p.id; // p.id = pageId подстраницы
            }
        }
    }

    pageMap = newMap;
    console.log('Сформировали карту подстраниц:', pageMap);
}

/**
 * 3.2) Скачиваем Excel‐файл (attachment) с указанной страницы
 */
async function fetchExcelFromConfluence(pageId) {
    const confluenceToken = process.env.CONFLUENCE_API_TOKEN;
    const baseUrl = 'https://wiki.sxl.team';

    // список вложений
    const attachmentsUrl = `${baseUrl}/rest/api/content/${pageId}/child/attachment`;
    const resp = await axios.get(attachmentsUrl, {
        headers: {
            'Authorization': `Bearer ${confluenceToken}`,
            'Accept': 'application/json'
        }
    });

    if (!resp.data || !resp.data.results || resp.data.results.length === 0) {
        throw new Error(`На странице ${pageId} вложений не найдено!`);
    }

    // Ищем первое .xlsx
    let attachment = resp.data.results.find(a =>
        a.metadata?.mediaType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );
    if (!attachment) {
        // Если нет чёткого .xlsx — берём первое
        attachment = resp.data.results[0];
    }

    const downloadUrl = attachment._links?.download;
    if (!downloadUrl) {
        throw new Error(`Не найдена ссылка download у вложения на странице ${pageId}`);
    }

    // Скачиваем файл (arraybuffer)
    const fileResp = await axios.get('https://wiki.sxl.team' + downloadUrl, {
        headers: { 'Authorization': `Bearer ${confluenceToken}` },
        responseType: 'arraybuffer'
    });

    return Buffer.from(fileResp.data);
}

/**
 * 3.3) Парсим содержимое Excel:
 *      Возвращаем schedule[ dayNum ] = { "9-21": [...], "10-19": [...], "21-9": [...] }
 */
function parseScheduleFromBuffer(xlsxBuffer) {
    const workbook = xlsx.read(xlsxBuffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];

    const raw = xlsx.utils.sheet_to_json(sheet, {
        header: 1, // массив массивов
        defval: ""
    });

    // ищем строку "ФИО"
    let headerRowIndex = -1;
    for (let i = 0; i < raw.length; i++) {
        const firstCell = String(raw[i][0] || "").trim().toLowerCase();
        if (firstCell === "фио") {
            headerRowIndex = i;
            break;
        }
    }
    if (headerRowIndex < 0) {
        throw new Error("В Excel не найдена строка, где первая ячейка = 'ФИО'");
    }

    // Определяем колонки для дней (1..31)
    const dayColumnMap = {};
    const headerRow = raw[headerRowIndex];
    for (let col = 1; col < headerRow.length; col++) {
        const val = String(headerRow[col] || "").trim();
        const dayNum = parseInt(val, 10);
        if (!isNaN(dayNum) && dayNum >= 1 && dayNum <= 31) {
            dayColumnMap[dayNum] = col;
        }
    }

    // Заготовка расписания
    const schedule = {};
    for (let d = 1; d <= 31; d++) {
        schedule[d] = {
            "9-21": [],
            "10-19": [],
            "21-9": []
        };
    }

    // Пропускаем строку с "вт, ср, чт" и т.д.
    let rowIndex = headerRowIndex + 2;

    for (; rowIndex < raw.length; rowIndex++) {
        const row = raw[rowIndex];
        if (!row || row.length === 0) break;

        const fioCell = String(row[0] || "").trim();
        if (!fioCell) break;

        const lowFio = fioCell.toLowerCase();
        if (
            lowFio.startsWith("итого человек")
            || lowFio.startsWith("итого работает")
            || lowFio.startsWith("с графиком")
            || lowFio.startsWith("итого в день")
            || lowFio === "фио"
        ) {
            // заканчиваем
            break;
        }

        for (const dStr in dayColumnMap) {
            const d = parseInt(dStr, 10);
            const colIndex = dayColumnMap[d];
            const cellVal = String(row[colIndex] || "").trim().toLowerCase().replace(/–/g, '-');

            if (cellVal === "9-21") {
                schedule[d]["9-21"].push(fioCell);
            } else if (cellVal === "10-19") {
                schedule[d]["10-19"].push(fioCell);
            } else if (cellVal === "21-9") {
                schedule[d]["21-9"].push(fioCell);
            }
            // отпуск/пусто — игнорируем
        }
    }

    return schedule;
}

/**
 * 3.4) Загрузить расписание для конкретного (year, month) из Confluence и сохранить в schedulesByKey
 */
async function loadScheduleForMonthYear(year, month) {
    const key = `${year}-${month}`;
    if (!pageMap[key]) {
        // нет в словаре pageMap
        console.warn(`Не найден pageId для "${year}-${month}". Возможно, нет подстраницы "График ..."?`);
        schedulesByKey[key] = {}; // пустое
        return;
    }

    const pageId = pageMap[key];
    const buffer = await fetchExcelFromConfluence(pageId);
    const scheduleObj = parseScheduleFromBuffer(buffer);
    schedulesByKey[key] = scheduleObj;
    console.log(`Расписание для ${key} (pageId=${pageId}) успешно загружено.`);
}

/**
 * Возвращаем объект расписания для нужного дня.
 * Если в кэше (schedulesByKey) нет подходящего месяца — подгружаем.
 */
async function getScheduleForDate(dt) {
    const y = dt.year;
    const m = dt.month;
    const key = `${y}-${m}`;

    // Если не загружено, попробовать загрузить
    if (!schedulesByKey[key]) {
        console.log(`[getScheduleForDate] Нет расписания для ${key}, пробуем загрузить...`);
        await loadScheduleForMonthYear(y, m); 
    }

    const scheduleObj = schedulesByKey[key] || {};
    const daySchedule = scheduleObj[dt.day];
    if (!daySchedule) {
        // нет данных на этот день
        return null;
    }
    return daySchedule;
}

// ----------------------------------------------------------------------------------
// 4) ПОЛУЧЕНИЕ ДЕЖУРНОГО (fetchDutyEngineer) — ПОЛНОСТЬЮ
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
// 5) JIRA: ПОЛУЧЕНИЕ И СОХРАНЕНИЕ ЗАДАЧ, ОТПРАВКА В ТГ
// ----------------------------------------------------------------------------------

async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira('sxl', 'https://jira.sxl.team/rest/api/2/search', process.env.JIRA_PAT_SXL, 'Техническая поддержка');
    await fetchAndStoreTasksFromJira('betone', 'https://jira.betone.team/rest/api/2/search', process.env.JIRA_PAT_BETONE, 'Техническая поддержка');
}

async function fetchAndStoreTasksFromJira(source, url, pat, ...departments) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);
        const departmentQuery = departments.map(dep => `"${dep}"`).join(" OR Отдел = ");
        let jql;

        if (source === 'sxl') {
            jql = `
                project = SUPPORT AND (
                    (issuetype = Infra AND status = "Open") OR
                    (issuetype = Office AND status = "Under review") OR
                    (issuetype = Office AND status = "Waiting for support") OR
                    (issuetype = Prod AND status = "Waiting for Developers approval") OR
                    (Отдел = ${departmentQuery} AND status = "Open")
                )
            `;
        } else {
            // betone
            jql = `project = SUPPORT AND (Отдел = ${departmentQuery}) AND status = "Open"`;
        }

        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            },
            params: { jql }
        });
        console.log(`${source} Jira API response:`, response.data);

        const fetchedTaskIds = response.data.issues.map(issue => `${source}-${issue.key}`);

        // Удаляем из локальной БД те, которых нет в свежем списке
        await new Promise((resolve, reject) => {
            const placeholders = fetchedTaskIds.map(() => '?').join(',');
            db.run(
                `DELETE FROM tasks
                 WHERE id NOT IN (${placeholders})
                   AND source = ?`,
                [...fetchedTaskIds, source],
                function(err) {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });

        // Добавляем/обновляем
        for (const issue of response.data.issues) {
            const uniqueId = `${source}-${issue.key}`;
            const task = {
                id: uniqueId,
                title: issue.fields.summary,
                priority: issue.fields.priority?.name || 'Не указан',
                issueType: issue.fields.issuetype?.name || 'Не указан',
                department: (
                    (source === 'betone' && issue.fields.customfield_10504)
                        ? issue.fields.customfield_10504.value
                        : (
                            (source === 'sxl' && issue.fields.customfield_10500)
                                ? issue.fields.customfield_10500.value
                                : 'Не указан'
                        )
                ),
                dateAdded: getMoscowTimestamp(),
                source
            };

            const existingTask = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM tasks WHERE id = ?', [uniqueId], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            if (existingTask) {
                db.run(
                    `UPDATE tasks SET
                        title = ?,
                        priority = ?,
                        issueType = ?,
                        department = ?,
                        source = ?
                      WHERE id = ?`,
                    [task.title, task.priority, task.issueType, task.department, task.source, task.id]
                );
            } else {
                db.run(
                    `INSERT OR REPLACE INTO tasks (id, title, priority, issueType, department, dateAdded, lastSent, source)
                     VALUES (?, ?, ?, ?, ?, ?, NULL, ?)`,
                    [task.id, task.title, task.priority, task.issueType, task.department, task.dateAdded, task.source]
                );
            }
        }
    } catch (error) {
        console.error(`Error fetching and storing tasks from ${source} Jira:`, error);
    }
}

async function getJiraTaskDetails(source, combinedId) {
    try {
        // Убираем префикс "betone-" или "sxl-" для реального ключа
        const realKey = extractRealJiraKey(combinedId); // Например, "SUPPORT-574"
        const url = `https://jira.${source}.team/rest/api/2/issue/${realKey}?fields=summary,description,attachment,priority,issuetype,status,assignee`;
        const pat = (source === 'sxl') ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        console.log(`[getJiraTaskDetails] GET ${url}`);
        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            }
        });
        return response.data; // объект issue от Jira
    } catch (error) {
        console.error(`[getJiraTaskDetails] Ошибка GET ${source}-${combinedId}:`, error);
        return null;
    }
}

// Отправляем задачи в ТГ (например, раз в сутки для техподдержки и раз в 3 дня для Infra/Office/Prod)
async function sendJiraTasks(ctx) {
    const today = getMoscowTimestamp().split(' ')[0];
    const query = `
        SELECT * FROM tasks WHERE
        (department = "Техническая поддержка" AND (lastSent IS NULL OR lastSent < date('${today}')))
        OR
        (issueType IN ('Infra', 'Office', 'Prod') AND (lastSent IS NULL OR lastSent < datetime('now', '-3 days')))
        ORDER BY CASE
            WHEN department = 'Техническая поддержка' THEN 1
            ELSE 2
        END
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('Error fetching tasks:', err);
            return;
        }

        for (const task of rows) {
            const keyboard = new InlineKeyboard();
            if (task.department === "Техническая поддержка") {
                keyboard
                    .text('Взять в работу', `take_task:${task.id}`)
                    .url('Перейти к задаче', getTaskUrl(task.source, task.id))
                    .text('⬇ Подробнее', `toggle_description:${task.id}`);
            } else if (['Infra', 'Office', 'Prod'].includes(task.issueType)) {
                keyboard
                    .url('Перейти к задаче', getTaskUrl(task.source, task.id))
                    .text('⬇ Подробнее', `toggle_description:${task.id}`);
            }

            const messageText =
                `Задача: ${task.id}\n` +
                `Источник: ${task.source}\n` +
                `Ссылка: ${getTaskUrl(task.source, task.id)}\n` +
                `Описание: ${task.title}\n` +
                `Приоритет: ${getPriorityEmoji(task.priority)}\n` +
                `Тип задачи: ${task.issueType}`;

            await ctx.reply(messageText, { reply_markup: keyboard });

            const moscowTimestamp = getMoscowTimestamp();
            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [moscowTimestamp, task.id]);
        }
    });
}

// ----------------------------------------------------------------------------------
// 6) ПРОВЕРКА НОВЫХ КОММЕНТАРИЕВ
// ----------------------------------------------------------------------------------

async function checkForNewComments() {
    try {
        const jql = `project = SUPPORT AND updated >= -7d`;
        const sources = ['sxl', 'betone'];
        const excludedAuthors = Object.values(jiraUserMappings).flatMap(mapping => Object.values(mapping));

        for (const source of sources) {
            const url = `https://jira.${source}.team/rest/api/2/search`;
            const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

            let startAt = 0;
            let total = 0;

            do {
                const response = await axios.get(url, {
                    headers: {
                        'Authorization': `Bearer ${pat}`,
                        'Accept': 'application/json'
                    },
                    params: {
                        jql,
                        maxResults: 50,
                        startAt,
                        fields: 'comment,assignee,summary,priority,issuetype,customfield_10500,customfield_10504'
                    }
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

                    // Уведомляем, только если отдел — Техподдержка, или автор — кто‐то из нас
                    if (!isTechSupportDept && !isOurComment) {
                        continue;
                    }

                    db.get(
                        'SELECT lastCommentId FROM task_comments WHERE taskId = ?',
                        [taskId],
                        (err, row) => {
                            if (err) {
                                console.error('Error fetching last comment from DB:', err);
                                return;
                            }

                            if (!row) {
                                // первый раз видим
                                sendTelegramMessage(taskId, source, issue, lastComment, author, department, isOurComment);
                                db.run(
                                    `INSERT INTO task_comments (taskId, lastCommentId, assignee)
                                     VALUES (?, ?, ?)`,
                                    [taskId, lastCommentId, issue.fields.assignee?.displayName || 'Не указан']
                                );
                            } else if (row.lastCommentId !== lastCommentId) {
                                // новый коммент
                                sendTelegramMessage(taskId, source, issue, lastComment, author, department, isOurComment);
                                db.run(
                                    `UPDATE task_comments
                                     SET lastCommentId = ?, assignee = ?
                                     WHERE taskId = ?`,
                                    [lastCommentId, issue.fields.assignee?.displayName || 'Не указан', taskId]
                                );
                            }
                        }
                    );
                }

                startAt += 50;
            } while (startAt < total);
        }
    } catch (error) {
        console.error('Error checking for new comments:', error);
    }
}

// Чтобы не заспамить — лимитируем отправку
const limiter = new Bottleneck({
    minTime: 5000,
    maxConcurrent: 1
});
const sendMessageWithLimiter = limiter.wrap(async (chatId, text, opts) => {
    await bot.api.sendMessage(chatId, text, opts);
});

const commentCache = {};

/**
 * Отправляет уведомление о новом комментарии.
 */
function sendTelegramMessage(combinedId, source, issue, lastComment, authorName, department, isOurComment) {
    const keyboard = new InlineKeyboard().url('Перейти к задаче', getTaskUrl(source, combinedId));
  
    // Преобразуем имя автора через маппинг
    let displayAuthor = authorName;
    const mappedAuthor = getHumanReadableName(authorName, source);
    if (mappedAuthor) {
      displayAuthor = mappedAuthor;
    }
  
    // Получаем HTML комментария через парсер
    const fullCommentHtml = parseCustomMarkdown(lastComment.body || '');
  
    // Обрезаем HTML безопасно, чтобы не обрезать тег посередине
    const MAX_LEN = 300; // пороговая длина в символах
    let shortCommentHtml = safeTruncateHtml(fullCommentHtml, MAX_LEN);
  
    // Если полный текст длиннее максимально допустимого, добавляем кнопку "Развернуть"
    if (fullCommentHtml.length > MAX_LEN) {
      keyboard.text('Развернуть', `expand_comment:${combinedId}:${lastComment.id}`);
    }
  
    // Префикс уведомления
    const prefix = isOurComment
      ? 'В задаче появился новый комментарий от технической поддержки:\n\n'
      : 'В задаче появился новый комментарий:\n\n';
  
    // Формируем «заголовок» уведомления (до блока "Комментарий:")
    const header =
      `<b>Задача:</b> ${combinedId}\n` +
      `<b>Источник:</b> ${source}\n` +
      `<b>Отдел:</b> ${department}\n` +
      `<b>Ссылка:</b> ${getTaskUrl(source, combinedId)}\n` +
      `<b>Описание:</b> ${escapeHtml(issue.fields.summary || '')}\n` +
      `<b>Приоритет:</b> ${getPriorityEmoji(issue.fields.priority?.name || 'Не указан')}\n` +
      `<b>Тип задачи:</b> ${escapeHtml(issue.fields.issuetype?.name || 'Не указан')}\n` +
      `<b>Исполнитель:</b> ${escapeHtml(issue.fields.assignee?.displayName || 'Не указан')}\n` +
      `<b>Автор комментария:</b> ${escapeHtml(displayAuthor)}\n` +
      `<b>Комментарий:</b>\n`;
  
    // Сохраняем в кэше header, оба варианта комментария и source для callback'ов
    const cacheKey = `${combinedId}:${lastComment.id}`;
    commentCache[cacheKey] = {
      header: prefix + header,
      shortHtml: shortCommentHtml,
      fullHtml: fullCommentHtml,
      source: source
    };
  
    const finalText = commentCache[cacheKey].header + shortCommentHtml;
  
    console.log('[DEBUG] Final message text to send:', finalText);
  
    sendMessageWithLimiter(process.env.ADMIN_CHAT_ID, finalText, {
      reply_markup: keyboard,
      parse_mode: 'HTML'
    }).catch(e => console.error('Error sending message to Telegram:', e));
  }
  
  
// Callback для разворачивания комментария
bot.callbackQuery(/^expand_comment:(.+):(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const combinedId = ctx.match[1];
    const commentId = ctx.match[2];
    const cacheKey = `${combinedId}:${commentId}`;
  
    const data = commentCache[cacheKey];
    if (!data) {
      return ctx.reply('Комментарий не найден в кеше (возможно, бот был перезапущен?)');
    }
  
    // Формируем новый текст: header + полный комментарий
    const newText = data.header + data.fullHtml;
    const keyboard = new InlineKeyboard()
      .text('Свернуть', `collapse_comment:${combinedId}:${commentId}`)
      .url('Перейти к задаче', getTaskUrl(data.source, combinedId));
  
    // Выводим новый текст (опционально добавьте лог)
    console.log('[DEBUG] Expand comment newText:', newText);
  
    await ctx.editMessageText(newText, {
      parse_mode: 'HTML',
      reply_markup: keyboard
    });
  } catch (err) {
    console.error('expand_comment error:', err);
    await ctx.reply('Ошибка при раскрытии комментария.');
  }
});
  
// Callback для сворачивания комментария
bot.callbackQuery(/^collapse_comment:(.+):(.+)$/, async (ctx) => {
  try {
    await ctx.answerCallbackQuery();
    const combinedId = ctx.match[1];
    const commentId = ctx.match[2];
    const cacheKey = `${combinedId}:${commentId}`;
  
    const data = commentCache[cacheKey];
    if (!data) {
      return ctx.reply('Комментарий не найден в кеше.');
    }
  
    // Формируем новый текст: header + короткий комментарий
    const newText = data.header + data.shortHtml;
    const keyboard = new InlineKeyboard()
      .text('Развернуть', `expand_comment:${combinedId}:${commentId}`)
      .url('Перейти к задаче', getTaskUrl(data.source, combinedId));
  
    console.log('[DEBUG] Collapse comment newText:', newText);
  
    await ctx.editMessageText(newText, {
      parse_mode: 'HTML',
      reply_markup: keyboard
    });
  } catch (err) {
    console.error('collapse_comment error:', err);
    await ctx.reply('Ошибка при сворачивании комментария.');
  }
});

  

// ----------------------------------------------------------------------------------
// 7) КНОПКА «ВЗЯТЬ В РАБОТУ»
// ----------------------------------------------------------------------------------
async function reassignIssueToRealUser(source, realJiraKey, telegramUsername) {
    try {
        // Находим Jira-логин из вашего словаря
        const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
        if (!jiraUsername) {
            console.log(`[reassignIssueToRealUser] Нет маппинга для ${telegramUsername} → Jira (source=${source})`);
            return false;
        }

        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
        const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${realJiraKey}/assignee`;

        const r = await axios.put(
            assigneeUrl,
            { name: jiraUsername },
            {
                headers: {
                    'Authorization': `Bearer ${pat}`,
                    'Content-Type': 'application/json'
                }
            }
        );

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
            if (!task) {
                return ctx.reply('Задача не найдена в БД.');
            }
            if (task.department !== "Техническая поддержка") {
                return ctx.reply('Эта задача не для ТП; нельзя взять в работу через бота.');
            }

            // 1) Делаем transition в Jira (как раньше)
            let success = false;
            try {
                success = await updateJiraTaskStatus(task.source, combinedId, telegramUsername);
            } catch (errUpd) {
                console.error('Ошибка updateJiraTaskStatus:', errUpd);
            }

            // Вставляем запись в user_actions, чтобы фиксировать, кто взял
            if (success) {
                db.run(
                    `INSERT INTO user_actions (username, taskId, action, timestamp)
                     VALUES (?, ?, ?, ?)`,
                    [telegramUsername, combinedId, 'take_task', getMoscowTimestamp()]
                );

                // Сообщим в чат
                const displayName = usernameMappings[telegramUsername] || telegramUsername;
                await ctx.reply(`OK, задачу ${combinedId} взял в работу: ${displayName}.`);

                // 2) Через 30 сек делаем *повторную* установку Assignee на настоящего исполнителя
                setTimeout(async () => {
                    const realKey = extractRealJiraKey(combinedId);
                    const reassignOk = await reassignIssueToRealUser(task.source, realKey, telegramUsername);
                    if (reassignOk) {
                        console.log(`Задача ${combinedId} (реально) переназначена на ${telegramUsername}`);
                    }
                }, 30_000);

            } else {
                await ctx.reply(`Не удалось перевести задачу ${combinedId} в нужный статус (updateJiraTaskStatus failed)`);
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

        // Назначаем исполнителя
        const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${realKey}/assignee`;
        const r1 = await axios.put(assigneeUrl, { name: jiraUsername }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json'
            }
        });
        if (r1.status !== 204) {
            console.error('Assignee error:', r1.status);
            return false;
        }

        // Переходим в нужный статус
        const transitionUrl = `https://jira.${source}.team/rest/api/2/issue/${realKey}/transitions`;
        const r2 = await axios.post(transitionUrl, {
            transition: { id: transitionId }
        }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json'
            }
        });
        return (r2.status === 204);
    } catch (error) {
        console.error(`Error updating Jira task:`, error);
        return false;
    }
}

// ----------------------------------------------------------------------------------
// 8) КНОПКА "Подробнее" (toggle_description)
// ----------------------------------------------------------------------------------

// 1. Вспомогательные функции

function escapeHtml(text) {
    if (!text) return '';
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }
  
  /**
   * Преобразует блоки:
   *   {code:java}...{code}
   *   {code}...{code}
   * в <pre><code class="language-...">...</code></pre>.
   */
  function convertCodeBlocks(input) {
    let output = input;
    // 1) {code:lang} ... {code}
    output = output.replace(/\{code:([\w\-]+)\}([\s\S]*?)\{code\}/g, (match, lang, code) => {
      return `<pre><code class="language-${lang}">${escapeHtml(code.trim())}</code></pre>`;
    });
  
    // 2) {code} ... {code}
    output = output.replace(/\{code\}([\s\S]*?)\{code\}/g, (match, code) => {
      return `<pre><code>${escapeHtml(code.trim())}</code></pre>`;
    });
    return output;
  }
  
  /**
   * Преобразует {noformat}...{noformat} → <pre>...</pre>.
   */
  function convertNoformatBlocks(text) {
    return text.replace(/\{noformat\}([\s\S]*?)\{noformat\}/g, (match, content) => {
      return `<pre>${escapeHtml(content.trim())}</pre>`;
    });
  }
  
  /**
   * Ищет [Link Text|URL] и превращает в <a href="URL">Link Text</a>.
   */
  function convertSquareBracketLinks(text) {
    // Пример: [View alert rule|https://example.com]
    return text.replace(/\[([^\|\]]+)\|([^\]]+)\]/g, (match, linkText, linkUrl) => {
      const safeText = escapeHtml(linkText.trim());
      const safeUrl = escapeHtml(linkUrl.trim());
      return `<a href="${safeUrl}">${safeText}</a>`;
    });
  }

  function safeTruncateHtml(html, maxLength) {
    if (html.length <= maxLength) return html;
    
    let truncated = html.slice(0, maxLength);
    // Считаем количество открывающих тегов <a и закрывающих </a>
    const openA = (truncated.match(/<a\b/gi) || []).length;
    const closeA = (truncated.match(/<\/a>/gi) || []).length;
    if (openA > closeA) {
      // Если тег <a> не закрыт, удаляем от последнего открытия до конца обрезанной строки
      const lastOpenIdx = truncated.lastIndexOf('<a');
      if (lastOpenIdx !== -1) {
        truncated = truncated.slice(0, lastOpenIdx);
      }
    }
    return truncated + '...';
  }
  
  
  /**
   * Преобразует строки, начинающиеся с "# ", в нумерованный список:
   * # item => "1) item", # another => "2) another", и т.д.
   */
  function convertHashLinesToNumbered(text) {
    let lines = text.split('\n');
  
    // Обязательно объявляем переменные:
    let result = [];
    let counter = 1;
  
    console.log('[DEBUG] Lines before processing:');
    for (let i = 0; i < lines.length; i++) {
      console.log(i, JSON.stringify(lines[i]), lines[i].split('').map(c => c.charCodeAt(0)));
    }
  
    for (let i = 0; i < lines.length; i++) {
      // Убираем неразрывные пробелы \u00A0 → обычные пробелы
      let line = lines[i].replace(/\u00A0/g, ' ');
      let trimmed = line.trim();
  
      // --- 1) Если строка – это просто "#" (или "# ")
      if (trimmed === '#' || trimmed === '#') {
        let nextIndex = i + 1;
        let foundText = null;
  
        while (nextIndex < lines.length) {
          let candidate = lines[nextIndex].replace(/\u00A0/g, ' ').trim();
          if (candidate) {
            foundText = candidate;
            break;
          }
          nextIndex++;
        }
  
        if (foundText) {
          // Склеиваем
          result.push(`${counter++}) ${foundText}`);
          // Пропускаем до строки nextIndex
          i = nextIndex;
        } else {
          result.push(`${counter++})`);
        }
      }
  
      // --- 2) Если строка начинается с "# " (в одной строке вместе с текстом)
      else if (trimmed.startsWith('# ')) {
        const content = trimmed.slice(2);
        result.push(`${counter++}) ${content}`);
      }
  
      // --- 3) Иначе — без изменений
      else {
        result.push(line);
      }
    }
  
    return result.join('\n');
  }
  
  
  /**
   * Преобразует "|col1|col2|" в <pre>|col1|col2|</pre>.
   * (Простейший вариант, если нужны более сложные таблицы, дописываем.)
   */
  function formatTables(text) {
    return text.replace(/\|(.+?)\|/g, match => {
      return `<pre>${escapeHtml(match.trim())}</pre>`;
    });
  }
  
  // 2. Основная функция "parseCustomMarkdown"
  // -----------------------------------------
  function parseCustomMarkdown(text) {
    if (!text) return '';
  
    // 1) {noformat}...{noformat}
    text = convertNoformatBlocks(text);
  
    // 2) {code}, {code:lang}
    text = convertCodeBlocks(text);
  
    // 3) "таблицы" |...|
    text = formatTables(text);
  
    // 4) [Text|URL]
    text = convertSquareBracketLinks(text);
  
    // 5) # lines => numbered
    text = convertHashLinesToNumbered(text);
  
    // 6) Markdown-like преобразования (*bold*, _italics_, +underline+, ~~strike~~, `inline code`)
    text = text
      // жирный
      .replace(/\*(.*?)\*/g, '<b>$1</b>')
      // курсив
      .replace(/_(.*?)_/g, '<i>$1</i>')
      // подчёркнутый
      .replace(/\+(.*?)\+/g, '<u>$1</u>')
      // зачёркнутый
      .replace(/~~(.*?)~~/g, '<s>$1</s>')
      // inline code: `...`
      .replace(/(^|\s)`([^`]+)`(\s|$)/g, '$1<code>$2</code>$3')
      // списки "- " и "* "
      .replace(/^\-\s(.*)/gm, '• $1')
      .replace(/^\*\s(.*)/gm, '• $1')
      // нумерованный 1. => "🔹 "
      .replace(/^\d+\.\s(.*)/gm, '🔹 $1')
      // Удаляем избыточные переносы (3+ подряд -> 2)
      .replace(/\n{3,}/g, '\n\n');
  
    return text;
  }
  
  /**
   * 3. Ваш "formatDescriptionAsHtml" просто вызывает parseCustomMarkdown
   */
  function formatDescriptionAsHtml(rawDescription) {
    return parseCustomMarkdown(rawDescription || '');
  }
  
  // ---------------------------------------------------------------------------
  // Пример использования
  // ---------------------------------------------------------------------------
  // const originalText = `# Hello world\n{code:java}\nSystem.out.println("Hi");\n{code}\n[Click|http://google.com]`;
  // const html = parseCustomMarkdown(originalText);
  // bot.api.sendMessage(chatId, html, { parse_mode: "HTML" });


  function getHumanReadableName(jiraName, source) {
    if (!jiraName || !source) return null;
    // Приводим значение к нижнему регистру и обрезаем пробелы
    const normalizedJiraName = jiraName.trim().toLowerCase();
    for (const [telegramUser, mapObj] of Object.entries(jiraUserMappings)) {
        // Приводим также ключ для сравнения
        if ((mapObj[source] || "").trim().toLowerCase() === normalizedJiraName) {
            return usernameMappings[telegramUser] || jiraName;
        }
    }
    return null;
}


bot.callbackQuery(/^toggle_description:(.+)$/, async (ctx) => {
    try {
        await ctx.answerCallbackQuery();
        const combinedId = ctx.match[1];  // например "betone-SUPPORT-574"

        // Сначала пытаемся узнать source (sxl/betone) из локальной БД
        let rowFromDb = await new Promise(resolve => {
            db.get('SELECT * FROM tasks WHERE id = ?', [combinedId], (err, row) => resolve(row));
        });

        let source = rowFromDb?.source;
        if (!source) {
            // fallback: пробуем вытащить из текста сообщения
            const txt = ctx.callbackQuery.message?.text || "";
            const match = txt.match(/Источник:\s*([^\n]+)/i);
            if (match) {
                source = match[1].trim();
            } else {
                // или берём первые 4-5 символов
                source = combinedId.split('-')[0]; // "betone" / "sxl"
            }
        }

        // 1) Делаем запрос в Jira, чтобы получить Актуальные данные задачи
        const issue = await getJiraTaskDetails(source, combinedId);
        if (!issue) {
            return ctx.reply('Не удалось получить данные задачи из Jira.');
        }

        // 2) Считываем нужные поля
        const summary      = issue.fields.summary       || 'Без названия';
        const description  = issue.fields.description   || 'Нет описания';
        const statusName   = issue.fields.status?.name  || '—';
        const priority     = issue.fields.priority?.name || 'None';
        const taskType     = issue.fields.issuetype?.name || '—';
        const assigneeObj  = issue.fields.assignee || null;
        
        // 3) Преобразуем приоритет в emoji (если надо)
        const priorityEmoji = getPriorityEmoji(priority);

        // 4) Определяем исполнителя
        let assigneeText = 'Никто';
        if (assigneeObj) {
            // Например, assigneeObj.name = "d.baratov"
            const mappedName = getHumanReadableName(assigneeObj.name, source);
            if (mappedName) {
                assigneeText = mappedName;
            } else {
                // Не из нашего отдела => берём displayName
                assigneeText = assigneeObj.displayName || assigneeObj.name;
            }
        }

        // 5) Проверяем, свернуто ли сейчас описание или развернуто
        const currentText = ctx.callbackQuery.message?.text.trimEnd() || "";
        const isExpanded = currentText.endsWith("..."); 
        // true, если сейчас уже "длинное описание" и в конце стоит "..."

        // 6) Кнопки
        const keyboard = new InlineKeyboard();
        if (rowFromDb?.department === "Техническая поддержка" && statusName === "Open") {
            keyboard.text('Взять в работу', `take_task:${combinedId}`);
        }
        keyboard
            .text(isExpanded ? 'Подробнее' : 'Скрыть', `toggle_description:${combinedId}`)
            .url('Открыть в Jira', `https://jira.${source}.team/browse/${extractRealJiraKey(combinedId)}`);

        // 7) Формируем текст
        if (!isExpanded) {
            // Сейчас «коротко» — при клике делаем «подробнее» (показать описание)
            const safeDesc = formatDescriptionAsHtml(description);
            await ctx.editMessageText(
                `<b>Задача:</b> ${combinedId}\n` +
                `<b>Источник:</b> ${source}\n` +
                `<b>Приоритет:</b> ${priorityEmoji}\n` +
                `<b>Тип задачи:</b> ${taskType}\n` +
                `<b>Заголовок:</b> ${escapeHtml(summary)}\n` +
                `<b>Исполнитель:</b> ${escapeHtml(assigneeText)}\n` +
                `<b>Статус:</b> ${escapeHtml(statusName)}\n\n` +
                `<b>Описание:</b>\n${safeDesc}\n\n...`,
                { parse_mode: 'HTML', reply_markup: keyboard }
            );
        } else {
            // Сейчас «подробно» — при клике сворачиваем
            await ctx.editMessageText(
                `<b>Задача:</b> ${combinedId}\n` +
                `<b>Источник:</b> ${source}\n` +
                `<b>Ссылка:</b> <a href="https://jira.${source}.team/browse/${extractRealJiraKey(combinedId)}">Открыть в Jira</a>\n` +
                `<b>Заголовок:</b> ${escapeHtml(summary)}\n` +
                `<b>Приоритет:</b> ${priorityEmoji}\n` +
                `<b>Тип задачи:</b> ${taskType}\n` +
                `<b>Исполнитель:</b> ${escapeHtml(assigneeText)}\n` +
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
// 9) ГЕНЕРАЦИЯ СООБЩЕНИЙ НА 10:00 И 21:00, И ЛОГИКА ПЕРЕКЛЮЧЕНИЯ МЕСЯЦЕВ
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

// ----------------------------------------------------------------------------------
// 10) РАСПИСАНИЕ CRON
// ----------------------------------------------------------------------------------

// (A) Каждую минуту — обновляем задачи из Jira и рассылаем
cron.schedule('* * * * *', async () => {
    try {
        console.log('[CRON] Обновление задач из Jira...');
        await fetchAndStoreJiraTasks();

        const ctx = {
            reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts)
        };
        await sendJiraTasks(ctx);
    } catch (err) {
        console.error('Ошибка в CRON fetchAndStoreJiraTasks/sendJiraTasks:', err);
    }
}, {
    timezone: 'Europe/Moscow'
});

// (B) Каждые 5 минут — проверка новых комментариев
cron.schedule('*/5 * * * *', async () => {
    try {
        console.log('[CRON] Проверка новых комментариев...');
        await checkForNewComments();
    } catch (err) {
        console.error('Ошибка в CRON checkForNewComments:', err);
    }
}, {
    timezone: 'Europe/Moscow'
});

// (C) 10:00 — утреннее сообщение из Excel
cron.schedule('0 10 * * *', async () => {
    try {
        const text = await getDayMessageText();
        await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, { parse_mode: 'HTML' });
    } catch (err) {
        console.error('[CRON 10:00] Ошибка:', err);
    }
}, { timezone: 'Europe/Moscow' });

// (D) 21:00 — вечернее сообщение
cron.schedule('0 21 * * *', async () => {
    try {
        const text = await getNightMessageText();
        await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, { parse_mode: 'HTML' });
    } catch (err) {
        console.error('[CRON 21:00] Ошибка:', err);
    }
}, { timezone: 'Europe/Moscow' });

// (E) Последний день месяца, 11:00 — подгружаем следующий месяц
cron.schedule('0 11 * * *', async () => {
    try {
        const now = getMoscowDateTime();
        const daysInMonth = now.daysInMonth;
        const today = now.day;
        if (today === daysInMonth) {
            // Текущий месяц +1
            const nextMonth = now.plus({ months: 1 });
            await bot.api.sendMessage(process.env.ADMIN_CHAT_ID,
                `Сегодня ${now.toFormat("dd.MM.yyyy")} — последний день месяца.\n` +
                `Подгружаем расписание на следующий месяц (${nextMonth.toFormat("LLLL yyyy")})...`
            );

            // Загрузим страницуMap заново, вдруг добавилась новая страница 
            await buildPageMapForSchedules();

            // Грузим расписание для nextMonth
            await loadScheduleForMonthYear(nextMonth.year, nextMonth.month);

            await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, '✅ Готово, расписание следующего месяца загружено.');
        }
    } catch (err) {
        console.error('[CRON LAST DAY] Ошибка:', err);
        bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Ошибка при загрузке расписания на следующий месяц');
    }
}, { timezone: 'Europe/Moscow' });

// ----------------------------------------------------------------------------------
// 11) ДОПОЛНИТЕЛЬНЫЕ КОМАНДЫ: /test_day, /test_night, /duty
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

// ----------------------------------------------------------------------------------
// 12) ИНИЦИАЛИЗАЦИЯ ПРИ СТАРТЕ
// ----------------------------------------------------------------------------------

async function initializeBotTasks() {
    console.log('[BOT INIT] Запуск задач...');

    // 1) Строим карту pageId (График апрель 2025 → 96732191 и т.п.)
    await buildPageMapForSchedules();

    // 2) Грузим расписание для "текущего" месяца
    const now = getMoscowDateTime();
    await loadScheduleForMonthYear(now.year, now.month);

    // 3) Подгружаем Jira
    await fetchAndStoreJiraTasks();

    // 4) Рассылаем задачи
    const ctx = { reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts) };
    await sendJiraTasks(ctx);

    // 5) Проверка новых комментариев
    await checkForNewComments();

    db.all('SELECT taskId FROM task_comments', [], (err, rows) => {
        if (err) console.error('Error fetching task_comments:', err);
        else console.log(`Total task_comments in DB: ${rows.length}`);
    });

    console.log('[BOT INIT] Всё готово.');
}

// Команды /start и /forcestart
bot.command('start', async (ctx) => {
    await ctx.reply('✅ Бот запущен. Все задачи работают. (/forcestart для повторного запуска)');
});

bot.command('forcestart', async (ctx) => {
    await initializeBotTasks();
    await ctx.reply('♻️ Все задачи были перезапущены (и расписание перечитано).');
});

// Стартуем
bot.start({
    onStart: (botInfo) => {
        console.log(`✅ Bot ${botInfo.username} is running`);
        initializeBotTasks();
    }
});
