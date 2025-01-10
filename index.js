require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const cron = require('node-cron');
const Bottleneck = require('bottleneck');

const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');

// Функция для получения текущего времени Москвы
function getMoscowTimestamp() {
    const moscowTime = DateTime.now().setZone('Europe/Moscow');
    return moscowTime.toFormat('yyyy-MM-dd HH:mm:ss');
}

// Обновляем структуру базы данных, добавляем поле issueType и таблицу task_comments
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

    // Новая таблица для хранения последних комментариев задач
    db.run(`CREATE TABLE IF NOT EXISTS task_comments (
        taskId TEXT PRIMARY KEY,
        lastCommentId TEXT,
        assignee TEXT,
        FOREIGN KEY(taskId) REFERENCES tasks(id)
    )`);
});

// Функция для получения эмодзи по приоритету
function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

// Функция для получения URL задачи
function getTaskUrl(source, taskId) {
    return `https://jira.${source}.team/browse/${taskId}`;
}

// Маппинги пользователей
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

// Функция для получения приоритетов и других полей задач из Jira
async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira('sxl', 'https://jira.sxl.team/rest/api/2/search', process.env.JIRA_PAT_SXL, 'Техническая поддержка');
    await fetchAndStoreTasksFromJira('betone', 'https://jira.betone.team/rest/api/2/search', process.env.JIRA_PAT_BETONE, 'Техническая поддержка');
}

// Функция для запроса и хранения задач из Jira
async function fetchAndStoreTasksFromJira(source, url, pat, ...departments) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);
        const departmentQuery = departments.map(dep => `"${dep}"`).join(" OR Отдел = ");
        let jql;
        if (source === 'sxl') {
            // JQL запрос для задач DevOps и Support
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
            // Запрос для Технической поддержки (betone)
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

        const fetchedTaskIds = response.data.issues.map(issue => issue.key);

        await new Promise((resolve, reject) => {
            const placeholders = fetchedTaskIds.map(() => '?').join(',');
            db.run(`DELETE FROM tasks WHERE id NOT IN (${placeholders}) AND source = ?`, [...fetchedTaskIds, source], function(err) {
                if (err) {
                    reject(err);
                    console.error(`Error deleting tasks from ${source} Jira:`, err);
                } else {
                    resolve();
                }
            });
        });

        for (const issue of response.data.issues) {
            const task = {
                id: issue.key,
                title: issue.fields.summary,
                priority: issue.fields.priority?.name || 'Не указан',
                issueType: issue.fields.issuetype?.name || 'Не указан',
                department: (source === 'betone' && issue.fields.customfield_10504) ? issue.fields.customfield_10504.value : ((source === 'sxl' && issue.fields.customfield_10500) ? issue.fields.customfield_10500.value : 'Не указан'),
                dateAdded: getMoscowTimestamp(),
                source: source
            };

            const existingTask = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM tasks WHERE id = ?', [task.id], (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(row);
                    }
                });
            });

            if (existingTask) {
                db.run('UPDATE tasks SET title = ?, priority = ?, issueType = ?, department = ?, source = ? WHERE id = ?', [task.title, task.priority, task.issueType, task.department, task.source, task.id]);
            } else {
                db.run('INSERT INTO tasks (id, title, priority, issueType, department, dateAdded, lastSent, source) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)', [task.id, task.title, task.priority, task.issueType, task.department, task.dateAdded, task.source]);
            }
        }
    } catch (error) {
        console.error(`Error fetching and storing tasks from ${source} Jira:`, error);
    }
}

// Функция для отправки задач в Telegram
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
                keyboard.text('Взять в работу', `take_task:${task.id}`);
                keyboard.url('Перейти к задаче', getTaskUrl(task.source, task.id));
            } else if (['Infra', 'Office', 'Prod'].includes(task.issueType)) {
                keyboard.url('Перейти к задаче', getTaskUrl(task.source, task.id));
            }

            const messageText = `Задача: ${task.id}
Источник: ${task.source}
Ссылка: ${getTaskUrl(task.source, task.id)}
Описание: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}`;
            console.log('Sending message to Telegram:', messageText);

            await ctx.reply(messageText, { reply_markup: keyboard });

            const moscowTimestamp = getMoscowTimestamp();
            console.log(`Updating lastSent for task ${task.id} to: ${moscowTimestamp}`);
            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [moscowTimestamp, task.id]);
        }
    });
}

async function checkForNewComments() {
    try {
        const jql = `project = SUPPORT AND Отдел = "Техническая поддержка" AND status in (Done, Awaiting, "Awaiting implementation") AND updated >= -2d`;
        const sources = ['sxl', 'betone'];

        // Получаем список авторов из jiraUserMappings
        const excludedAuthors = Object.values(jiraUserMappings)
            .flatMap(mapping => Object.values(mapping));

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
                    params: { jql, maxResults: 50, startAt, fields: 'comment,assignee,summary,priority,issuetype' }
                });

                const issues = response.data.issues;
                total = response.data.total;

                for (const issue of issues) {
                    const taskId = issue.key;

                    // Получаем последний комментарий
                    const comments = issue.fields.comment.comments;
                    if (comments.length === 0) continue;

                    const lastComment = comments[comments.length - 1];
                    const lastCommentId = lastComment.id;
                    const author = lastComment.author?.name || 'Не указан';

                    // Если задача новая или комментарий обновился
                    db.get('SELECT lastCommentId FROM task_comments WHERE taskId = ?', [taskId], (err, row) => {
                        if (err) {
                            console.error('Error fetching last comment from DB:', err);
                            return;
                        }

                        if (!row) {
                            // Если задача новая и комментарий не от excludedAuthors, отправляем сообщение
                            if (!excludedAuthors.includes(author)) {
                                sendTelegramMessage(taskId, source, issue, lastComment, author);
                            }

                            // Добавляем задачу в базу
                            db.run(
                                'INSERT INTO task_comments (taskId, lastCommentId, assignee) VALUES (?, ?, ?)',
                                [taskId, lastCommentId, issue.fields.assignee?.displayName || 'Не указан']
                            );
                        } else if (row.lastCommentId !== lastCommentId) {
                            // Если комментарий новый и не от excludedAuthors, отправляем сообщение
                            if (!excludedAuthors.includes(author)) {
                                sendTelegramMessage(taskId, source, issue, lastComment, author);
                            }

                            // Обновляем комментарий в базе
                            db.run(
                                'UPDATE task_comments SET lastCommentId = ?, assignee = ? WHERE taskId = ?',
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

const limiter = new Bottleneck({
    minTime: 2000, // Минимум 2 секунды между запросами
    maxConcurrent: 1 // Один запрос одновременно
});

// Оборачиваем функцию отправки сообщений в лимитер
const sendMessageWithLimiter = limiter.wrap(async (chatId, messageText, options) => {
    try {
        console.log(`Sending message to Telegram: ${messageText}`);
        await bot.api.sendMessage(chatId, messageText, options);
    } catch (error) {
        console.error('Error in sendMessageWithLimiter:', error);
        throw error;
    }
});

// Функция для отправки сообщения в Telegram
function sendTelegramMessage(taskId, source, issue, lastComment, author) {
    const keyboard = new InlineKeyboard();
    keyboard.url('Перейти к задаче', getTaskUrl(source, taskId));

    const messageText = `В задаче появился новый комментарий:

Задача: ${taskId}
Источник: ${source}
Ссылка: ${getTaskUrl(source, taskId)}
Описание: ${issue.fields.summary}
Приоритет: ${getPriorityEmoji(issue.fields.priority?.name || 'Не указан')}
Тип задачи: ${issue.fields.issuetype?.name || 'Не указан')}
Исполнитель: ${issue.fields.assignee?.displayName || 'Не указан'}
Автор комментария: ${author}
Комментарий: ${lastComment.body}`;

    sendMessageWithLimiter(process.env.ADMIN_CHAT_ID, messageText, {
        reply_markup: keyboard
    }).catch(err => {
        console.error('Error sending message to Telegram:', err);
    });
}


cron.schedule('*/5 * * * *', () => {
    console.log('Checking for new comments...');
    checkForNewComments();
});


bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
    try {
        const taskId = ctx.match[1];
        const username = ctx.from.username;

        db.get('SELECT * FROM tasks WHERE id = ?', [taskId], async (err, task) => {
            if (err) {
                console.error('Ошибка при получении задачи из базы данных:', err);
                await ctx.reply('Произошла ошибка при обработке вашего запроса.');
                return;
            }

            if (!task) {
                await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
                await ctx.reply('Задача не найдена.');
                return;
            }

            if (task.department === "Техническая поддержка") {
                const success = await updateJiraTaskStatus(task.source, taskId, username);
                if (success) {
                    const displayName = usernameMappings[ctx.from.username] || ctx.from.username;
                    const messageText = `Задача: ${task.id}
Источник: ${task.source}
Ссылка: ${getTaskUrl(task.source, task.id)}
Описание: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Отдел: ${task.department}
Взял в работу: ${displayName}
`;

                    const keyboard = new InlineKeyboard().url('Перейти к задаче', getTaskUrl(task.source, task.id));

                    await ctx.editMessageText(messageText, { reply_markup: { inline_keyboard: [] } });

                    db.run('INSERT INTO user_actions (username, taskId, action, timestamp) VALUES (?, ?, ?, ?)',
                        [ctx.from.username, taskId, 'take_task', getMoscowTimestamp()]);
                } else {
                    await ctx.reply(`Не удалось обновить статус задачи ${taskId}. Попробуйте снова.`);
                }
            } else {
                await ctx.reply('Эта задача не для отдела Технической поддержки и не может быть взята в работу через этот бот.');
            }
        });
    } catch (error) {
        console.error('Ошибка в обработчике take_task:', error);
        await ctx.reply('Произошла ошибка при обработке вашего запроса.');
    }
});

// Функция для обновления статуса задачи в Jira
async function updateJiraTaskStatus(source, taskId, telegramUsername) {
    try {
        let transitionId;
        if (source === 'sxl') {
            transitionId = '221'; // Ваш transitionId для sxl
        } else if (source === 'betone') {
            transitionId = '201'; // Ваш transitionId для betone
        } else {
            console.error('Invalid source specified');
            return false;
        }

        const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
        if (!jiraUsername) {
            console.error(`No Jira username mapping found for Telegram username: ${telegramUsername}`);
            return false;
        }

        const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${taskId}/assignee`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
        
        const assigneeResponse = await axios.put(assigneeUrl, {
            name: jiraUsername
        }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json'
            }
        });

        if (assigneeResponse.status !== 204) {
            console.error(`Error assigning Jira task: ${assigneeResponse.status}`);
            return false;
        }

        const transitionUrl = `https://jira.${source}.team/rest/api/2/issue/${taskId}/transitions`;
        const transitionResponse = await axios.post(transitionUrl, {
            transition: {
                id: transitionId
            }
        }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json'
            }
        });

        return transitionResponse.status === 204;
    } catch (error) {
        console.error(`Error updating ${source} Jira task:, error`);
        return false;
    }
}


let interval = null;
let nightShiftCron = null;
let morningShiftCron = null;


// Команда /start для запуска бота
bot.command('start', async (ctx) => {
    await ctx.reply('Привет! Каждую минуту я буду проверять новые задачи...');

    if (!interval) {
        interval = setInterval(async () => {
            console.log('Interval triggered. Sending Jira tasks...');
            await fetchAndStoreJiraTasks();
            await sendJiraTasks(ctx);
            console.log('Jira tasks sent.');
        }, 60000);
    } else {
        await ctx.reply('Интервал уже запущен.');
    }

    if (!nightShiftCron) {
        nightShiftCron = cron.schedule('0 01 * * *', async () => {
            await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброй ночи! Заполни тикет передачи смены.');
        }, {
            scheduled: true,
            timezone: "Europe/Moscow"
        });

        if (!morningShiftCron) {
            morningShiftCron = cron.schedule('0 10 * * *', async () => {
                await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброе утро! Не забудь проверить задачи на сегодня: заполни тикет передачи смены.');
            }, {
                scheduled: true,
                timezone: "Europe/Moscow"
            });
        }

        nightShiftCron.start();
        morningShiftCron.start();
    }

    // Проверяем наличие комментариев и выполняем вставку или обновление
    db.all('SELECT taskId FROM task_comments', [], async (err, rows) => {
        if (err) {
            console.error('Error fetching task comments:', err);
            return;
        }
        console.log(`Total task_comments in database: ${rows.length}`);
    });
});

bot.start();