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
const xlsx = require('xlsx'); // Для чтения Excel-файлов

// ----------------------------------------------------------------------------------
// 1) ИНИЦИАЛИЗАЦИЯ БОТА, БАЗЫ, ФУНКЦИЙ
// ----------------------------------------------------------------------------------

const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');

// Получение московского времени в формате "yyyy-MM-dd HH:mm:ss"
function getMoscowTimestamp() {
    return DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd HH:mm:ss');
}

// Получение DateTime с часовым поясом Москвы
function getMoscowDateTime() {
    return DateTime.now().setZone('Europe/Moscow');
}

// Создаём нужные таблицы в SQLite (если их ещё нет)
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

// Приоритет → эмодзи
function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

// Вспомогательная функция, которая отделяет "sxl-" или "betone-" от реального ключа
function extractRealJiraKey(fullId) {
    // Пример: "sxl-SUPPORT-123" → ["sxl", "SUPPORT", "123"] → realKey = "SUPPORT-123"
    const parts = fullId.split('-');
    parts.shift(); // убираем первый элемент (source)
    return parts.join('-');
}

// Генерация URL для Jira
function getTaskUrl(source, combinedId) {
    const realKey = extractRealJiraKey(combinedId);
    return `https://jira.${source}.team/browse/${realKey}`;
}

// Маппинг Telegram username → ФИО (пример)
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
// 2) ЗАПУСКАЕМ EXPRESS ДЛЯ РАЗДАЧИ ВЛОЖЕНИЙ
// ----------------------------------------------------------------------------------

const app = express();
const ATTACHMENTS_DIR = path.join(__dirname, 'attachments');

if (!fs.existsSync(ATTACHMENTS_DIR)) {
    fs.mkdirSync(ATTACHMENTS_DIR);
}
app.use('/attachments', express.static(ATTACHMENTS_DIR));

// Допустим, запускаем на порту 3000
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Express server listening on port ${PORT}`);
});

// ----------------------------------------------------------------------------------
// 3) КРОН ДЛЯ УДАЛЕНИЯ СТАРЫХ ФАЙЛОВ (в 3:00)
// ----------------------------------------------------------------------------------

cron.schedule('0 3 * * *', () => {
    console.log('[CRON] Удаляем старые файлы из attachments...');
    const now = Date.now();
    const cutoff = now - 24 * 60 * 60 * 1000; // старше суток

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
// 4) ФУНКЦИИ ДЛЯ RABOTЫ С JIRA
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
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        });

        // Обновляем / добавляем задачи
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
        const realKey = extractRealJiraKey(combinedId);
        const url = `https://jira.${source}.team/rest/api/2/issue/${realKey}?fields=summary,description,attachment,priority,issuetype,status`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            }
        });
        return response.data;
    } catch (error) {
        console.error(`Ошибка при получении данных задачи ${combinedId} из Jira (${source}):`, error);
        return null;
    }
}

// ----------------------------------------------------------------------------------
// 5) ОТПРАВКА ЗАДАЧ ИЗ JIRA В TELEGRAM
// ----------------------------------------------------------------------------------

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
                    if (!comments || comments.length === 0) {
                        continue;
                    }

                    const lastComment = comments[comments.length - 1];
                    const lastCommentId = lastComment.id;
                    const author = lastComment.author?.name || 'Не указан';

                    // Уведомляем, если:
                    //  1) department === "Техническая поддержка", ИЛИ
                    //  2) author ∈ наш список excludedAuthors
                    const isTechSupportDept = (department === 'Техническая поддержка');
                    const isOurComment = excludedAuthors.includes(author);

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
                                // Нет записи => первый раз видим
                                sendTelegramMessage(taskId, source, issue, lastComment, author, department, isOurComment);
                                db.run(
                                    `INSERT INTO task_comments (taskId, lastCommentId, assignee)
                                     VALUES (?, ?, ?)`,
                                    [taskId, lastCommentId, issue.fields.assignee?.displayName || 'Не указан']
                                );
                            } else if (row.lastCommentId !== lastCommentId) {
                                // Новый комментарий => уведомляем
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

// Лимитер на отправку
const limiter = new Bottleneck({
    minTime: 5000,
    maxConcurrent: 1
});
const sendMessageWithLimiter = limiter.wrap(async (chatId, text, opts) => {
    await bot.api.sendMessage(chatId, text, opts);
});

function sendTelegramMessage(combinedId, source, issue, lastComment, author, department, isOurComment) {
    const keyboard = new InlineKeyboard().url('Перейти к задаче', getTaskUrl(source, combinedId));

    const prefix = isOurComment
        ? 'В задаче появился новый комментарий от технической поддержки:\n\n'
        : 'В задаче появился новый комментарий:\n\n';

    const msg =
        prefix +
        `Задача: ${combinedId}\n` +
        `Источник: ${source}\n` +
        `Отдел: ${department}\n` +
        `Ссылка: ${getTaskUrl(source, combinedId)}\n` +
        `Описание: ${issue.fields.summary}\n` +
        `Приоритет: ${getPriorityEmoji(issue.fields.priority?.name || 'Не указан')}\n` +
        `Тип задачи: ${issue.fields.issuetype?.name || 'Не указан'}\n` +
        `Исполнитель: ${issue.fields.assignee?.displayName || 'Не указан'}\n` +
        `Автор комментария: ${author}\n` +
        `Комментарий: ${lastComment.body}`;

    sendMessageWithLimiter(process.env.ADMIN_CHAT_ID, msg, { reply_markup: keyboard })
        .catch(e => console.error('Error sending message to Telegram:', e));
}

// ----------------------------------------------------------------------------------
// 7) КНОПКА "ВЗЯТЬ В РАБОТУ"
// ----------------------------------------------------------------------------------

bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
    try {
        await ctx.answerCallbackQuery();
        const combinedId = ctx.match[1];
        const username = ctx.from.username;

        db.get('SELECT * FROM tasks WHERE id = ?', [combinedId], async (err, task) => {
            if (err) {
                console.error('Ошибка при получении задачи:', err);
                const keyboard = new InlineKeyboard()
                    .text('Подробнее', `toggle_description:${combinedId}`)
                    .url('Перейти к задаче', getTaskUrl('sxl', combinedId));
                return ctx.reply('Произошла ошибка при получении задачи.', { reply_markup: keyboard });
            }

            if (!task) {
                const keyboard = new InlineKeyboard()
                    .text('Подробнее', `toggle_description:${combinedId}`)
                    .url('Перейти к задаче', getTaskUrl('sxl', combinedId));

                try {
                    await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
                } catch {}
                return ctx.reply('Задача не найдена в БД.', { reply_markup: keyboard });
            }

            if (task.department === "Техническая поддержка") {
                let success = false;
                try {
                    success = await updateJiraTaskStatus(task.source, combinedId, username);
                } catch (errUpd) {
                    console.error('Ошибка updateJiraTaskStatus:', errUpd);
                }

                const displayName = usernameMappings[username] || username;
                const keyboard = new InlineKeyboard()
                    .text('Подробнее', `toggle_description:${task.id}`)
                    .url('Перейти к задаче', getTaskUrl(task.source, task.id));

                if (success) {
                    const msg =
                        `Задача: ${task.id}\n` +
                        `Источник: ${task.source}\n` +
                        `Ссылка: ${getTaskUrl(task.source, task.id)}\n` +
                        `Описание: ${task.title}\n` +
                        `Приоритет: ${getPriorityEmoji(task.priority)}\n` +
                        `Отдел: ${task.department}\n` +
                        `Взял в работу: ${displayName}`;

                    try {
                        await ctx.editMessageText(msg, { reply_markup: keyboard });
                    } catch (e) {
                        console.error('Ошибка editMessageText:', e);
                    }

                    db.run(
                        `INSERT INTO user_actions (username, taskId, action, timestamp)
                         VALUES (?, ?, ?, ?)`,
                        [username, combinedId, 'take_task', getMoscowTimestamp()]
                    );
                } else {
                    await ctx.reply(
                        `Не удалось обновить статус задачи ${task.id}. Попробуйте снова.`,
                        { reply_markup: keyboard }
                    );
                }
            } else {
                const keyboard = new InlineKeyboard()
                    .text('Подробнее', `toggle_description:${task.id}`)
                    .url('Перейти к задаче', getTaskUrl(task.source, task.id));
                await ctx.reply(
                    'Эта задача не для отдела Технической поддержки и не может быть взята в работу через этот бот.',
                    { reply_markup: keyboard }
                );
            }
        });
    } catch (error) {
        console.error('Ошибка в take_task:', error);
        const keyboard = new InlineKeyboard()
            .text('Подробнее', `toggle_description:${ctx.match[1]}`)
            .url('Перейти к задаче', getTaskUrl('sxl', ctx.match[1]));

        await ctx.reply('Произошла ошибка.', { reply_markup: keyboard });
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

function escapeHtml(text) {
    if (!text) return '';
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function formatTables(text) {
    return text.replace(/\|(.+?)\|/g, match => `<pre>${match.trim()}</pre>`);
}

function convertCodeBlocks(text) {
    return text
        .replace(/\{code:([\w\-]+)\}([\s\S]*?)\{code\}/g, (match, lang, code) => {
            return `<pre><code class="language-${lang}">${escapeHtml(code.trim())}</code></pre>`;
        })
        .replace(/\{code\}([\s\S]*?)\{code\}/g, (match, code) => {
            return `<pre><code>${escapeHtml(code.trim())}</code></pre>`;
        });
}

function parseCustomMarkdown(text) {
    if (!text) return '';

    text = convertCodeBlocks(text);
    text = formatTables(text);

    return text
        .replace(/\*(.*?)\*/g, '<b>$1</b>')
        .replace(/_(.*?)_/g, '<i>$1</i>')
        .replace(/\+(.*?)\+/g, '<u>$1</u>')
        .replace(/~~(.*?)~~/g, '<s>$1</s>')
        .replace(/(^|\s)`([^`]+)`(\s|$)/g, '$1<code>$2</code>$3')
        .replace(/^\-\s(.*)/gm, '• $1')
        .replace(/^\*\s(.*)/gm, '• $1')
        .replace(/^\d+\.\s(.*)/gm, '🔹 $1')
        .replace(/\n{3,}/g, '\n\n');
}

function formatDescriptionAsHtml(rawDescription) {
    return parseCustomMarkdown(rawDescription || '');
}

bot.callbackQuery(/^toggle_description:(.+)$/, async (ctx) => {
    try {
        await ctx.answerCallbackQuery();
        const combinedId = ctx.match[1];

        let task = await new Promise(resolve => {
            db.get('SELECT * FROM tasks WHERE id = ?', [combinedId], (err, row) => resolve(row));
        });

        let source, issue;

        if (task) {
            source = task.source;
            issue = await getJiraTaskDetails(source, combinedId);
        }

        // Если не удалось — пробуем явно sxl, betone
        if (!issue) {
            issue = await getJiraTaskDetails('sxl', combinedId);
            if (issue) source = 'sxl';
            else {
                issue = await getJiraTaskDetails('betone', combinedId);
                if (issue) source = 'betone';
            }
        }

        if (!issue || !source) {
            await ctx.reply('Не удалось загрузить данные из Jira.');
            return;
        }

        const summary = issue.fields.summary || 'Нет заголовка';
        const fullDescription = issue.fields.description || 'Нет описания';
        const priorityEmoji = getPriorityEmoji(issue.fields.priority?.name || 'Не указан');
        const taskType = issue.fields.issuetype?.name || 'Не указан';
        const taskUrl = getTaskUrl(source, combinedId);
        const taskStatus = issue.fields.status?.name;

        const safeSummary = escapeHtml(summary);
        const safeDescription = formatDescriptionAsHtml(fullDescription);
        const safeTitle = escapeHtml(task?.title || summary);

        const userAction = await new Promise(resolve => {
            db.get(
                `SELECT * FROM user_actions
                 WHERE taskId = ?
                   AND action = "take_task"
                 ORDER BY timestamp DESC LIMIT 1`,
                [combinedId],
                (err, row) => resolve(row)
            );
        });

        const isTaken = !!userAction;
        const takenBy = isTaken ? (usernameMappings[userAction.username] || userAction.username) : 'Никто';

        const currentText = ctx.callbackQuery.message?.text.trimEnd() || "";
        const isExpanded = currentText.endsWith("...");

        const keyboard = new InlineKeyboard();
        if ((task?.department === "Техническая поддержка") && (!isTaken || taskStatus === "Open")) {
            keyboard.text('Взять в работу', `take_task:${combinedId}`);
        }
        keyboard
            .text(isExpanded ? 'Подробнее' : 'Скрыть', `toggle_description:${combinedId}`)
            .url('Открыть в Jira', taskUrl);

        if (!isExpanded) {
            // При раскрытии добавляем ссылки на вложения
            if (issue.fields.attachment && Array.isArray(issue.fields.attachment)) {
                let counter = 1;
                for (const att of issue.fields.attachment) {
                    try {
                        const fileResp = await axios.get(att.content, {
                            responseType: 'arraybuffer',
                            headers: {
                                'Authorization': `Bearer ${
                                    source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE
                                }`
                            }
                        });

                        let originalFilename = att.filename.replace(/[^\w.\-]/g, '_').substring(0, 100);
                        const finalName = `${uuidv4()}_${originalFilename}`;
                        const filePath = path.join(ATTACHMENTS_DIR, finalName);
                        fs.writeFileSync(filePath, fileResp.data);

                        const publicUrl = `${process.env.PUBLIC_BASE_URL}/attachments/${finalName}`;

                        keyboard.row().url(`Вложение #${counter++}`, publicUrl);
                    } catch (errAttach) {
                        console.error('Ошибка при скачивании вложения:', errAttach);
                    }
                }
            }

            await ctx.editMessageText(
                `<b>Задача:</b> ${combinedId}\n` +
                `<b>Источник:</b> ${source}\n` +
                `<b>Приоритет:</b> ${priorityEmoji}\n` +
                `<b>Тип задачи:</b> ${taskType}\n` +
                `<b>Заголовок:</b> ${safeSummary}\n` +
                `<b>Взята в работу:</b> ${takenBy}\n\n` +
                `<b>Описание:</b>\n${safeDescription}\n\n...`,
                { parse_mode: 'HTML', reply_markup: keyboard }
            );
        } else {
            await ctx.editMessageText(
                `<b>Задача:</b> ${combinedId}\n` +
                `<b>Источник:</b> ${source}\n` +
                `<b>Ссылка:</b> <a href="${taskUrl}">${taskUrl}</a>\n` +
                `<b>Описание:</b> ${safeTitle}\n` +
                `<b>Приоритет:</b> ${priorityEmoji}\n` +
                `<b>Тип задачи:</b> ${taskType}\n` +
                `<b>Взята в работу:</b> ${takenBy}\n`,
                { parse_mode: 'HTML', reply_markup: keyboard }
            );
        }
    } catch (error) {
        console.error('Ошибка в toggle_description:', error);
        await ctx.reply('Произошла ошибка при обработке вашего запроса.');
    }
});

// ----------------------------------------------------------------------------------
// 9) ИНТЕГРАЦИЯ С CONFLUENCE (пример команды /duty, если нужно)
// ----------------------------------------------------------------------------------

async function fetchDutyEngineer() {
    try {
        const pageId = '3539406'; // пример ID страницы Confluence
        const token = process.env.CONFLUENCE_API_TOKEN;

        const resp = await axios.get(`https://wiki.sxl.team/rest/api/2/content/${pageId}?expand=body.view`, {
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

        // Пример преобразования:
        // ...
        return 'Не найдено (пример)';
    } catch (error) {
        console.error('Ошибка при запросе к Confluence:', error);
        throw error;
    }
}

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
// 10) МНОГО ФАЙЛОВ, scheduleByMonthYear[year][month][day] = ...
// ----------------------------------------------------------------------------------

// Словарь для распознавания месяцев из имени файла: "mart", "april", "may", ...
const monthWords = {
    january: 1,
    february: 2,
    mart: 3,
    april: 4,
    may: 5,
    june: 6,
    july: 7,
    august: 8,
    september: 9,
    october: 10,
    november: 11,
    december: 12,
};

const scheduleByMonthYear = {}; 
// структура: scheduleByMonthYear[год][месяц][dayNum] = { '9-21': [...], '10-19': [...], '21-9': [...] }

// Парсим все .xlsx файлы в папке raspisanie
function loadAllSchedules() {
    const dirPath = path.join(__dirname, 'raspisanie');
    if (!fs.existsSync(dirPath)) {
        console.warn(`Папка 'raspisanie' не найдена`);
        return;
    }

    const files = fs.readdirSync(dirPath).filter(f => f.toLowerCase().endsWith('.xlsx'));
    const re = /^([a-zA-Zа-яА-Я]+)_(\d{4})\.xlsx$/;

    for (const file of files) {
        const match = re.exec(file);
        if (!match) {
            console.warn(`Файл ${file} не соответствует шаблону "<monthWord>_<year>.xlsx"`);
            continue;
        }
        const monthWord = match[1].toLowerCase(); // "mart", "april", ...
        const yearNum = parseInt(match[2], 10);   // 2025, например

        const monthNum = monthWords[monthWord]; // lookup
        if (!monthNum) {
            console.warn(`Неизвестный месяц '${monthWord}' в файле ${file}`);
            continue;
        }

        // Парсим Excel
        const filePath = path.join(dirPath, file);
        const scheduleForMonth = parseOneExcelFile(filePath);

        // Заполняем scheduleByMonthYear
        if (!scheduleByMonthYear[yearNum]) {
            scheduleByMonthYear[yearNum] = {};
        }
        scheduleByMonthYear[yearNum][monthNum] = scheduleForMonth;
    }
}

/**
 * Парсит ОДИН Excel-файл с расписанием:
 * - Ищет строку, где первая ячейка = "ФИО".
 * - Далее считает, что [1..N] = дни (1..31).
 * - Формирует объект: day => { '9-21': [...], '10-19': [...], '21-9': [...] }
 */
function parseOneExcelFile(filePath) {
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];

    const raw = xlsx.utils.sheet_to_json(sheet, { header: 1, defval: "" });

    let headerRowIndex = -1;
    for (let i = 0; i < raw.length; i++) {
        if (String(raw[i][0]).trim().toLowerCase() === "фио") {
            headerRowIndex = i;
            break;
        }
    }
    if (headerRowIndex === -1) {
        console.warn(`В файле ${path.basename(filePath)} не найдена строка "ФИО"`);
        return {};
    }

    // dayColumnMap[day] = colIndex
    const dayColumnMap = {};
    for (let col = 1; col < raw[headerRowIndex].length; col++) {
        const cellVal = String(raw[headerRowIndex][col]).trim();
        const dayNum = parseInt(cellVal, 10);
        if (!isNaN(dayNum) && dayNum >= 1 && dayNum <= 31) {
            dayColumnMap[dayNum] = col;
        }
    }

    const schedule = {};
    for (let d = 1; d <= 31; d++) {
        schedule[d] = {
            "9-21": [],
            "10-19": [],
            "21-9": []
        };
    }

    // ниже идут строки с ФИО
    for (let i = headerRowIndex + 1; i < raw.length; i++) {
        const row = raw[i];
        if (!row || row.length === 0) continue;

        const fio = String(row[0]).trim();
        if (!fio) continue;

        for (const dayStr of Object.keys(dayColumnMap)) {
            const day = parseInt(dayStr, 10);
            const colIndex = dayColumnMap[day];
            const cellVal = String(row[colIndex] || "").trim().toLowerCase();

            if (cellVal === "9-21" || cellVal === "9–21") {
                schedule[day]["9-21"].push(fio);
            } else if (cellVal === "10-19" || cellVal === "10–19") {
                schedule[day]["10-19"].push(fio);
            } else if (cellVal === "21-9" || cellVal === "21–9") {
                schedule[day]["21-9"].push(fio);
            } else {
                // отпуск, пусто, пропуск
            }
        }
    }

    return schedule;
}

/**
 * Функция, чтобы получить расписание для конкретной даты (DateTime).
 * Если нет файла для year/month, возвращаем null. Иначе – объект вида:
 *   { dayNum => { '9-21': [...], '10-19': [...], '21-9': [...] } }
 */
function getScheduleForDate(dt) {
    const year = dt.year;
    const month = dt.month;
    const day = dt.day;

    const scheduleForYear = scheduleByMonthYear[year];
    if (!scheduleForYear) {
        return null;
    }
    const scheduleForMonth = scheduleForYear[month];
    if (!scheduleForMonth) {
        return null;
    }
    return scheduleForMonth[day] || null;
}

// ----------------------------------------------------------------------------------
// 11) УТРО И ВЕЧЕР ИЗ РАСПИСАНИЯ
// ----------------------------------------------------------------------------------

function getDayMessageText() {
    const now = getMoscowDateTime();
    const daySchedule = getScheduleForDate(now);
    if (!daySchedule) {
        return `Расписание на сегодня (${now.toFormat("dd.MM.yyyy")}) не найдено.`;
    }

    const arr9_21 = daySchedule["9-21"] || [];
    const arr10_19 = daySchedule["10-19"] || [];
    const arr21_9 = daySchedule["21-9"] || [];

    return `🔔 <b>Расписание на сегодня, ${now.toFormat("dd.MM.yyyy")} (10:00)</b>\n` +
           `\n<b>Дневная (9-21):</b> ${arr9_21.length ? arr9_21.join(", ") : "—"}\n` +
           `<b>Дневная 5/2 (10-19):</b> ${arr10_19.length ? arr10_19.join(", ") : "—"}\n` +
           `<b>Сегодня в ночь (21-9):</b> ${arr21_9.length ? arr21_9.join(", ") : "—"}\n`;
}

function getNightMessageText() {
    const now = getMoscowDateTime();
    const todaySchedule = getScheduleForDate(now) || {};
    
    const tomorrow = now.plus({ days: 1 });
    const tomorrowSchedule = getScheduleForDate(tomorrow) || {};

    const arr21_9_today = todaySchedule["21-9"] || [];
    const arr9_21_tomorrow = tomorrowSchedule["9-21"] || [];
    const arr10_19_tomorrow = tomorrowSchedule["10-19"] || [];

    return `🌙 <b>Расписание вечер, ${now.toFormat("dd.MM.yyyy")} (21:00)</b>\n` +
           `\n<b>Сегодня в ночь (21-9):</b> ${arr21_9_today.length ? arr21_9_today.join(", ") : "—"}\n` +
           `<b>Завтра утро (9-21):</b> ${arr9_21_tomorrow.length ? arr9_21_tomorrow.join(", ") : "—"}\n` +
           `<b>Завтра 5/2 (10-19):</b> ${arr10_19_tomorrow.length ? arr10_19_tomorrow.join(", ") : "—"}\n`;
}

// ----------------------------------------------------------------------------------
// 12) КРОН ЗАДАЧИ (10:00, 21:00, последний день месяца в 11:00) + старые nightShiftCron/morningShiftCron
// ----------------------------------------------------------------------------------

// 10:00 — сообщение из Excel

cron.schedule('* * * * *', async () => {
    try {
        console.log('[CRON] Обновление задач из Jira (каждую минуту)...');
        await fetchAndStoreJiraTasks();
        // Если нужно сразу же рассылать новые задачи, то можете вызвать sendJiraTasks:
        // const ctx = { reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts) };
        // await sendJiraTasks(ctx);
    } catch (err) {
        console.error('Ошибка в CRON fetchAndStoreJiraTasks:', err);
    }
}, {
    timezone: 'Europe/Moscow'
});

// ---------------------------------------------------------------------------
// КРОН каждые 5 минут – проверка новых комментариев
// ---------------------------------------------------------------------------
cron.schedule('*/5 * * * *', async () => {
    try {
        console.log('[CRON] Проверка новых комментариев (каждые 5 минут)...');
        await checkForNewComments();
    } catch (err) {
        console.error('Ошибка в CRON checkForNewComments:', err);
    }
}, {
    timezone: 'Europe/Moscow'
});


cron.schedule('0 10 * * *', () => {
    try {
        const text = getDayMessageText();
        bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, { parse_mode: 'HTML' });
    } catch (err) {
        console.error('[CRON 10:00] Ошибка:', err);
    }
}, { timezone: 'Europe/Moscow' });

// 21:00 — сообщение из Excel
cron.schedule('0 21 * * *', () => {
    try {
        const text = getNightMessageText();
        bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, { parse_mode: 'HTML' });
    } catch (err) {
        console.error('[CRON 21:00] Ошибка:', err);
    }
}, { timezone: 'Europe/Moscow' });

// Последний день месяца в 11:00 (напоминание)
cron.schedule('0 11 * * *', () => {
    const now = getMoscowDateTime();
    const daysInMonth = now.daysInMonth;
    const today = now.day;
    if (today === daysInMonth) {
        bot.api.sendMessage(
            process.env.ADMIN_CHAT_ID,
            `Сегодня ${now.toFormat("dd.MM.yyyy")} — последний день месяца.\n` +
            `Не забудьте загрузить новое расписание в папку "raspisanie"!`
        );
    }
}, { timezone: 'Europe/Moscow' });

// Дополнительные команды /test_day и /test_night
bot.command('test_day', async (ctx) => {
    try {
        const text = getDayMessageText();
        await ctx.reply(text, { parse_mode: 'HTML' });
    } catch (err) {
        console.error('Ошибка /test_day:', err);
        await ctx.reply('Ошибка при формировании дневного сообщения');
    }
});

bot.command('test_night', async (ctx) => {
    try {
        const text = getNightMessageText();
        await ctx.reply(text, { parse_mode: 'HTML' });
    } catch (err) {
        console.error('Ошибка /test_night:', err);
        await ctx.reply('Ошибка при формировании вечернего сообщения');
    }
});

// Старые nightShiftCron / morningShiftCron:
let nightShiftCron = null;
let morningShiftCron = null;

function setupOldShiftCrons() {
    if (!nightShiftCron) {
        nightShiftCron = cron.schedule('0 1 * * *', async () => {
            await bot.api.sendMessage(
                process.env.ADMIN_CHAT_ID,
                'Доброй ночи! Заполни тикет передачи смены.'
            );
        }, { scheduled: true, timezone: 'Europe/Moscow' });
    }

    if (!morningShiftCron) {
        morningShiftCron = cron.schedule('0 10 * * *', async () => {
            try {
                // Пример: дополнительное утреннее сообщение "Доброе утро..."
                const engineer = await fetchDutyEngineer();
                await bot.api.sendMessage(
                    process.env.ADMIN_CHAT_ID,
                    `Доброе утро! Проверь задачи.\nДежурный специалист: ${engineer}`
                );
            } catch (err) {
                console.error('Ошибка при получении дежурного:', err);
            }
        }, { scheduled: true, timezone: 'Europe/Moscow' });
    }
}

// ----------------------------------------------------------------------------------
// 13) СТАРТ БОТА (initializeBotTasks, /start, /forcestart)
// ----------------------------------------------------------------------------------

async function initializeBotTasks() {
    console.log('[BOT INIT] Автоматический запуск задач...');

    // 1) Загружаем все расписания
    loadAllSchedules();

    // 2) Fetch Jira
    await fetchAndStoreJiraTasks();

    // 3) Отправляем задачи
    const ctx = { reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts) };
    await sendJiraTasks(ctx);

    // 4) Проверяем комментарии
    checkForNewComments();

    // 5) Запускаем "старые" крон-задачи 1:00 / 10:00
    setupOldShiftCrons();

    db.all('SELECT taskId FROM task_comments', [], async (err, rows) => {
        if (err) {
            console.error('Error fetching task_comments:', err);
            return;
        }
        console.log(`Total task_comments in database: ${rows.length}`);
    });

    console.log('[BOT INIT] Все задачи успешно запущены.');
}

bot.command('start', async (ctx) => {
    await ctx.reply('✅ Бот работает. Все задачи запущены. Для перезапуска: /forcestart');
});

bot.command('forcestart', async (ctx) => {
    await initializeBotTasks();
    await ctx.reply('♻️ Все задачи были запущены повторно вручную (и расписание перечитано).');
});

bot.start({
    onStart: (botInfo) => {
      console.log(`✅ Bot ${botInfo.username} is up and running`);
      initializeBotTasks();
    }
  });
  
