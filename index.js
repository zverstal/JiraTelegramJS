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

// ----------------------------------------------------------------------------------
// 1) ИНИЦИАЛИЗАЦИЯ БОТА, БАЗЫ, ФУНКЦИЙ
// ----------------------------------------------------------------------------------

const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');

// Получение московского времени
function getMoscowTimestamp() {
    return DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd HH:mm:ss');
}

// Создаём нужные таблицы в SQLite
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

// Генерация URL для Jira
function getTaskUrl(source, taskId) {
    return `https://jira.${source}.team/browse/${taskId}`;
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

// Создаём папку, если нет
if (!fs.existsSync(ATTACHMENTS_DIR)) {
    fs.mkdirSync(ATTACHMENTS_DIR);
}

// Раздаём статику по пути /attachments
app.use('/attachments', express.static(ATTACHMENTS_DIR));

// Допустим, запускаем на порту 3000
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Express server listening on port ${PORT}`);
});

// ----------------------------------------------------------------------------------
// 3) КРОН ДЛЯ УДАЛЕНИЯ СТАРЫХ ФАЙЛОВ РАЗ В СУТКИ (в 3:00)
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

// 4.1) Фетчим задачи из Jira (сразу из 2 источников)
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

        const fetchedTaskIds = response.data.issues.map(i => i.key);

        // Удаляем из локальной БД лишние
        await new Promise((resolve, reject) => {
            const placeholders = fetchedTaskIds.map(() => '?').join(',');
            db.run(
                `DELETE FROM tasks WHERE id NOT IN (${placeholders}) AND source = ?`,
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
            const task = {
                id: issue.key,
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
                db.get('SELECT * FROM tasks WHERE id = ?', [task.id], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            if (existingTask) {
                db.run(
                    `UPDATE tasks SET title=?, priority=?, issueType=?, department=?, source=? WHERE id=?`,
                    [task.title, task.priority, task.issueType, task.department, task.source, task.id]
                );
            } else {
                db.run(
                    `INSERT INTO tasks (id, title, priority, issueType, department, dateAdded, lastSent, source)
                     VALUES (?, ?, ?, ?, ?, ?, NULL, ?)`,
                    [task.id, task.title, task.priority, task.issueType, task.department, task.dateAdded, task.source]
                );
            }
        }
    } catch (error) {
        console.error(`Error fetching and storing tasks from ${source} Jira:`, error);
    }
}

async function getJiraTaskDetails(source, taskId) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}?fields=summary,description,attachment,priority,issuetype,status`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            }
        });
        return response.data;
    } catch (error) {
        console.error(`Ошибка при получении данных задачи ${taskId} из Jira (${source}):`, error);
        return null;
    }
}


// ----------------------------------------------------------------------------------
// 5) ОТПРАВКА ЗАДАЧ В TELEGRAM
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
        const jql = `project = SUPPORT AND Отдел = "Техническая поддержка" AND status in (Done, Awaiting, "Awaiting implementation") AND updated >= -2d`;
        const sources = ['sxl', 'betone'];

        const excludedAuthors = Object.values(jiraUserMappings).flatMap(mapping => Object.values(mapping));

        for (const source of sources) {
            const url = `https://jira.${source}.team/rest/api/2/search`;
            const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

            let startAt = 0;
            let total = 0;

            do {
                const response = await axios.get(url, {
                    headers: { 'Authorization': `Bearer ${pat}`, 'Accept': 'application/json' },
                    params: { jql, maxResults: 50, startAt, fields: 'comment,assignee,summary,priority,issuetype' }
                });

                total = response.data.total;
                const issues = response.data.issues;

                for (const issue of issues) {
                    const taskId = issue.key;
                    const comments = issue.fields.comment.comments;
                    if (!comments || comments.length === 0) continue;

                    const lastComment = comments[comments.length - 1];
                    const lastCommentId = lastComment.id;
                    const author = lastComment.author?.name || 'Не указан';

                    db.get('SELECT lastCommentId FROM task_comments WHERE taskId = ?', [taskId], (err, row) => {
                        if (err) {
                            console.error('Error fetching last comment from DB:', err);
                            return;
                        }

                        if (!row) {
                            // нет записи, значит первый раз
                            if (!excludedAuthors.includes(author)) {
                                sendTelegramMessage(taskId, source, issue, lastComment, author);
                            }
                            db.run(
                                `INSERT INTO task_comments (taskId, lastCommentId, assignee) VALUES (?, ?, ?)`,
                                [taskId, lastCommentId, issue.fields.assignee?.displayName || 'Не указан']
                            );
                        } else if (row.lastCommentId !== lastCommentId) {
                            // новый коммент
                            if (!excludedAuthors.includes(author)) {
                                sendTelegramMessage(taskId, source, issue, lastComment, author);
                            }
                            db.run(
                                `UPDATE task_comments SET lastCommentId = ?, assignee = ? WHERE taskId = ?`,
                                [lastCommentId, issue.fields.assignee?.displayName || 'Не указан', taskId]
                            );
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

// Лимитер на отправку
const limiter = new Bottleneck({
    minTime: 2000,
    maxConcurrent: 1
});
const sendMessageWithLimiter = limiter.wrap(async (chatId, text, opts) => {
    await bot.api.sendMessage(chatId, text, opts);
});

// Отправляем сообщение при новом комментарии
function sendTelegramMessage(taskId, source, issue, lastComment, author) {
    const keyboard = new InlineKeyboard().url('Перейти к задаче', getTaskUrl(source, taskId));

    const msg =
        `В задаче появился новый комментарий:\n\n` +
        `Задача: ${taskId}\n` +
        `Источник: ${source}\n` +
        `Ссылка: ${getTaskUrl(source, taskId)}\n` +
        `Описание: ${issue.fields.summary}\n` +
        `Приоритет: ${getPriorityEmoji(issue.fields.priority?.name || 'Не указан')}\n` +
        `Тип задачи: ${issue.fields.issuetype?.name || 'Не указан'}\n` +
        `Исполнитель: ${issue.fields.assignee?.displayName || 'Не указан'}\n` +
        `Автор комментария: ${author}\n` +
        `Комментарий: ${lastComment.body}`;

    sendMessageWithLimiter(process.env.ADMIN_CHAT_ID, msg, { reply_markup: keyboard })
        .catch(e => console.error('Error sending message to Telegram:', e));
}

// Проверяем новые комментарии каждые 5 минут
cron.schedule('*/5 * * * *', () => {
    console.log('Checking for new comments...');
    checkForNewComments();
});

// ----------------------------------------------------------------------------------
// 7) КНОПКА "ВЗЯТЬ В РАБОТУ"
// ----------------------------------------------------------------------------------
bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
    try {
        await ctx.answerCallbackQuery();
        const taskId = ctx.match[1];
        const username = ctx.from.username;

        db.get('SELECT * FROM tasks WHERE id = ?', [taskId], async (err, task) => {
            if (err) {
                console.error('Ошибка при получении задачи:', err);
                const keyboard = new InlineKeyboard()
                    .text('Подробнее', `toggle_description:${taskId}`)
                    .url('Перейти к задаче', getTaskUrl('sxl', taskId));
                return ctx.reply('Произошла ошибка при получении задачи.', { reply_markup: keyboard });
            }

            if (!task) {
                const keyboard = new InlineKeyboard()
                    .text('Подробнее', `toggle_description:${taskId}`)
                    .url('Перейти к задаче', getTaskUrl('sxl', taskId));

                try {
                    await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
                } catch {}

                return ctx.reply('Задача не найдена в БД.', { reply_markup: keyboard });
            }

            if (task.department === "Техническая поддержка") {
                let success = false;
                try {
                    success = await updateJiraTaskStatus(task.source, taskId, username);
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

                    db.run(`INSERT INTO user_actions (username, taskId, action, timestamp) VALUES (?, ?, ?, ?)`,
                        [username, taskId, 'take_task', getMoscowTimestamp()]);
                } else {
                    await ctx.reply(`Не удалось обновить статус задачи ${taskId}. Попробуйте снова.`, { reply_markup: keyboard });
                }
            } else {
                const keyboard = new InlineKeyboard()
                    .text('Подробнее', `toggle_description:${task.id}`)
                    .url('Перейти к задаче', getTaskUrl(task.source, task.id));

                await ctx.reply('Эта задача не для отдела Технической поддержки и не может быть взята в работу через этот бот.', { reply_markup: keyboard });
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

// Функция обновления статуса
async function updateJiraTaskStatus(source, taskId, telegramUsername) {
    try {
        let transitionId = source === 'sxl' ? '221' : '201'; // Пример
        const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
        if (!jiraUsername) {
            console.error(`No Jira username for telegram user: ${telegramUsername}`);
            return false;
        }
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        // Назначаем исполнителя
        const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${taskId}/assignee`;
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

        // Делаем переход
        const transitionUrl = `https://jira.${source}.team/rest/api/2/issue/${taskId}/transitions`;
        const r2 = await axios.post(transitionUrl, {
            transition: { id: transitionId }
        }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json'
            }
        });
        return r2.status === 204;
    } catch (error) {
        console.error(`Error updating Jira task:`, error);
        return false;
    }
}

// Функция экранирования HTML
function escapeHtml(text) {
    if (!text) return '';
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

// Функция обработки таблиц (оборачиваем в <pre></pre>)
function formatTables(text) {
    return text.replace(/\|(.+?)\|/g, match => `<pre>${match.trim()}</pre>`);
}

// Функция обработки блоков кода
function convertCodeBlocks(text) {
    return text
        .replace(/\{code:([\w\-]+)\}([\s\S]*?)\{code\}/g, (match, lang, code) => {
            return `<pre><code class="language-${lang}">${escapeHtml(code.trim())}</code></pre>`;
        })
        .replace(/\{code\}([\s\S]*?)\{code\}/g, (match, code) => {
            return `<pre><code>${escapeHtml(code.trim())}</code></pre>`;
        });
}

// Функция преобразования Markdown в HTML
function parseCustomMarkdown(text) {
    if (!text) return '';

    text = convertCodeBlocks(text); // Обрабатываем блоки кода
    text = formatTables(text); // Обрабатываем таблицы

    return text
        .replace(/\*(.*?)\*/g, '<b>$1</b>')  // *Жирный*
        .replace(/_(.*?)_/g, '<i>$1</i>')    // _Курсив_
        .replace(/\+(.*?)\+/g, '<u>$1</u>')      // __Подчеркнутый__
        .replace(/~~(.*?)~~/g, '<s>$1</s>')      // ~~Зачеркнутый~~
        .replace(/(^|\s)`([^`]+)`(\s|$)/g, '$1<code>$2</code>$3') // `Инлайн-код`
        .replace(/^\-\s(.*)/gm, '• $1')         // - Маркированный список
        .replace(/^\*\s(.*)/gm, '• $1')         // * Альтернативный маркированный список
        .replace(/^\d+\.\s(.*)/gm, '🔹 $1')     // 1. Нумерованный список
        .replace(/\n{3,}/g, '\n\n');            // Убираем повторяющиеся \n\n\n
}

// Функция обработки описания
function formatDescriptionAsHtml(rawDescription) {
    return parseCustomMarkdown(rawDescription || '');
}

bot.callbackQuery(/^toggle_description:(.+)$/, async (ctx) => {
    try {
        await ctx.answerCallbackQuery();
        const taskId = ctx.match[1];

        let task = await new Promise(resolve => {
            db.get('SELECT * FROM tasks WHERE id = ?', [taskId], (err, row) => resolve(row));
        });

        let source;
        let issue;

        // Попробуем получить задачу из базы
        if (task) {
            source = task.source;
            issue = await getJiraTaskDetails(source, taskId);
        }

        // Если задачи в базе нет, или не удалось её получить по source, пробуем явно оба источника
        if (!issue) {
            issue = await getJiraTaskDetails('sxl', taskId);
            if (issue) {
                source = 'sxl';
            } else {
                issue = await getJiraTaskDetails('betone', taskId);
                if (issue) {
                    source = 'betone';
                }
            }
        }

        // Если после попыток задача всё равно не найдена:
        if (!issue || !source) {
            await ctx.reply('Не удалось загрузить данные из Jira.');
            return;
        }

        const summary = issue.fields.summary || 'Нет заголовка';
        const fullDescription = issue.fields.description || 'Нет описания';
        const priorityEmoji = getPriorityEmoji(issue.fields.priority?.name || 'Не указан');
        const taskType = issue.fields.issuetype?.name || 'Не указан';
        const taskUrl = getTaskUrl(source, taskId);
        const taskStatus = issue.fields.status?.name;

        const safeSummary = escapeHtml(summary);
        const safeDescription = formatDescriptionAsHtml(fullDescription);
        const safeTitle = escapeHtml(task?.title || summary);

        const userAction = await new Promise(resolve => {
            db.get('SELECT * FROM user_actions WHERE taskId = ? AND action = "take_task" ORDER BY timestamp DESC LIMIT 1', [taskId], (err, row) => resolve(row));
        });

        const isTaken = !!userAction;
        const takenBy = isTaken ? (usernameMappings[userAction.username] || userAction.username) : 'Никто';

        const currentText = ctx.callbackQuery.message?.text.trimEnd() || "";
        const isExpanded = currentText.endsWith("...");

        const keyboard = new InlineKeyboard();

        if ((task?.department === "Техническая поддержка") && (!isTaken || taskStatus === "Open")) {
            keyboard.text('Взять в работу', `take_task:${taskId}`);
        }

        keyboard
            .text(isExpanded ? 'Подробнее' : 'Скрыть', `toggle_description:${taskId}`)
            .url('Открыть в Jira', taskUrl);

        if (!isExpanded) {
            let counter = 1;
            for (const att of issue.fields.attachment || []) {
                try {
                    const fileResp = await axios.get(att.content, {
                        responseType: 'arraybuffer',
                        headers: {
                            'Authorization': `Bearer ${source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE}`
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

            await ctx.editMessageText(
                `<b>Задача:</b> ${taskId}\n` +
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
                `<b>Задача:</b> ${taskId}\n` +
                `<b>Источник:</b> ${source}\n` +
                `<b>Ссылка:</b> <a href=\"${taskUrl}\">${taskUrl}<\/a>\n` +
                `<b>Описание:</b> ${safeTitle}\n` +
                `<b>Приоритет:</b> ${priorityEmoji}\n` +
                `<b>Тип задачи:</b> ${taskType}\n` +
                `<b>Взята в работу:</b> ${takenBy}\n`,
                { parse_mode: 'HTML', reply_markup: keyboard }
            );
        }
    } catch (error) {
        console.error('Ошибка в обработчике toggle_description:', error);
        await ctx.reply('Произошла ошибка при обработке вашего запроса.');
    }
});



// ----------------------------------------------------------------------------------
// 9) ИНТЕГРАЦИЯ С CONFLUENCE (ДЕЖУРНЫЙ)
// ----------------------------------------------------------------------------------

async function fetchDutyEngineer() {
    try {
        const pageId = '3539406'; // пример ID страницы Confluence
        const token = process.env.CONFLUENCE_API_TOKEN;

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

        // Обрезаем HTML до элемента с "2024", чтобы игнорировать расписание 2024 года
        const marker = '<span class="expand-control-text conf-macro-render">2024</span>';
        const markerIndex = html.indexOf(marker);
        if (markerIndex !== -1) {
            html = html.slice(0, markerIndex);
        }

        // Парсинг строк таблицы с расписанием для 2025
        const rowRegex = /<(?:tr|TR)[^>]*>\s*<td[^>]*>(\d+)<\/td>\s*<td[^>]*>(\d{2}\.\d{2}-\d{2}\.\d{2})<\/td>\s*<td[^>]*>([^<]+)<\/td>/g;
        const schedule = [];
        let match;
        while ((match = rowRegex.exec(html)) !== null) {
            schedule.push({
                index: match[1],
                range: match[2],
                name: match[3].trim()
            });
        }

        if (schedule.length === 0) {
            console.log('Не удалось извлечь расписание дежурств из HTML.');
            return 'Не найдено';
        }

        // Получаем текущую дату в часовом поясе Москвы
        const now = DateTime.now().setZone("Europe/Moscow");

        // Определяем начало недели (понедельник) и конец недели (воскресенье)
        const startOfWeek = now.startOf('week');
        const endOfWeek = startOfWeek.plus({ days: 6 });
        const currentYear = startOfWeek.year;

        // Ищем запись, где диапазон совпадает с текущей неделей
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

            // Если начало недели и конец недели совпадают с диапазоном расписания, возвращаем имя
            if (startOfWeek.day === scheduleStart.day &&
                startOfWeek.month === scheduleStart.month &&
                endOfWeek.day === scheduleEnd.day &&
                endOfWeek.month === scheduleEnd.month) {
                return item.name;
            }
        }
        return 'Не найдено';
    } catch (error) {
        console.error('Ошибка при запросе к Confluence:', error);
        throw error;
    }
}

// Пример использования в команде бота
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
// 10 и 11) СТАРТ БОТА С АВТОЗАПУСКОМ ЗАДАЧ И ВОЗМОЖНОСТЬЮ РУЧНОГО ПЕРЕЗАПУСКА
// ----------------------------------------------------------------------------------

let interval = null;
let nightShiftCron = null;
let morningShiftCron = null;

async function initializeBotTasks() {
    console.log('[BOT INIT] Автоматический запуск задач...');

    if (!interval) {
        interval = setInterval(async () => {
            console.log('Interval triggered. Fetching + Sending Jira tasks...');
            await fetchAndStoreJiraTasks();

            const ctx = { reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts) };
            await sendJiraTasks(ctx);
            console.log('Jira tasks sent.');
        }, 60000);
    }

    if (!nightShiftCron) {
        nightShiftCron = cron.schedule('0 1 * * *', async () => {
            await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброй ночи! Заполни тикет передачи смены.');
        }, { scheduled: true, timezone: 'Europe/Moscow' });
    }

    if (!morningShiftCron) {
        morningShiftCron = cron.schedule('0 10 * * *', async () => {
            try {
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

    cron.schedule('*/5 * * * *', () => {
        console.log('Checking for new comments...');
        checkForNewComments();
    });

    await fetchAndStoreJiraTasks();
    const ctx = { reply: (text, opts) => bot.api.sendMessage(process.env.ADMIN_CHAT_ID, text, opts) };
    await sendJiraTasks(ctx);

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
    await ctx.reply('✅ Бот уже работает. Все задачи запущены. Если хочешь запустить задачи повторно, используй /forcestart');
});

bot.command('forcestart', async (ctx) => {
    await initializeBotTasks();
    await ctx.reply('♻️ Все задачи были запущены повторно вручную.');
});

bot.start({
    onStart: initializeBotTasks
});