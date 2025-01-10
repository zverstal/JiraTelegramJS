require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const cron = require('node-cron');

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
        const jql = `project = SUPPORT AND Отдел = "Техническая поддержка" AND resolution = Done And status in (Done, Awaiting, "Awaiting implementation") and updated >= -30d`;
        const sources = ['sxl', 'betone'];

        for (const source of sources) {
            const url = `https://jira.${source}.team/rest/api/2/search`;
            const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

            const response = await axios.get(url, {
                headers: {
                    'Authorization': `Bearer ${pat}`,
                    'Accept': 'application/json'
                },
                params: { jql, fields: 'comment,assignee,summary,priority,issuetype' }
            });

            const issues = response.data.issues;

            for (const issue of issues) {
                const taskId = issue.key;

                // Получаем последний комментарий
                const comments = issue.fields.comment.comments;
                if (comments.length === 0) continue;

                const lastComment = comments[comments.length - 1];
                const lastCommentId = lastComment.id;

                // Получаем исполнителя задачи
                const assignee = issue.fields.assignee ? issue.fields.assignee.displayName : 'Не указан';

                // Получаем последний сохраненный комментарий из базы
                db.get('SELECT lastCommentId FROM task_comments WHERE taskId = ?', [taskId], (err, row) => {
                    if (err) {
                        console.error('Error fetching last comment from DB:', err);
                        return;
                    }

                    if (!row) {
                        // Первый запуск: сохраняем последний комментарий и assignee
                        db.run(
                            'INSERT INTO task_comments (taskId, lastCommentId, assignee) VALUES (?, ?, ?)',
                            [taskId, lastCommentId, assignee]
                        );
                    } else if (row.lastCommentId !== lastCommentId) {
                        // Новый комментарий найден, обновляем lastCommentId и assignee
                        db.run(
                            'UPDATE task_comments SET lastCommentId = ?, assignee = ? WHERE taskId = ?',
                            [lastCommentId, assignee, taskId]
                        );

                        // Отправляем сообщение в бот
                        const keyboard = new InlineKeyboard();
                        keyboard.url('Перейти к задаче', getTaskUrl(source, taskId));

                        const messageText = `Задача: ${taskId}
Источник: ${source}
Ссылка: ${getTaskUrl(source, taskId)}
Описание: ${issue.fields.summary}
Приоритет: ${issue.fields.priority?.name || 'Не указан'}
Тип задачи: ${issue.fields.issuetype?.name || 'Не указан'}
Исполнитель: ${assignee}
Автор комментария: ${lastComment.author.displayName}
Комментарий: ${lastComment.body}`;

                        bot.api.sendMessage(process.env.ADMIN_CHAT_ID, messageText, {
                            reply_markup: keyboard
                        }).catch(err => {
                            console.error('Error sending message to Telegram:', err);
                        });
                    }
                });
            }
        }
    } catch (error) {
        console.error('Error checking for new comments:', error);
    }
}

cron.schedule('*/5 * * * *', () => {
    console.log('Checking for new comments...');
    checkForNewComments();
});

bot.command('report', async (ctx) => {
    const query = `
        SELECT assignee, COUNT(taskId) AS taskCount
        FROM task_comments
        WHERE taskId IN (
            SELECT id
            FROM task_comments
            WHERE lastCommentId IS NOT NULL
        )
        GROUP BY assignee
        ORDER BY taskCount DESC
    `;

    db.all(query, [], (err, rows) => {
        if (err) {
            console.error('Error generating report:', err);
            ctx.reply('Произошла ошибка при генерации отчета.');
            return;
        }

        if (rows.length === 0) {
            ctx.reply('Нет данных для формирования отчета.');
            return;
        }

        let reportText = 'Отчет по завершенным задачам:\n\n';
        rows.forEach((row) => {
            const displayName = row.assignee || 'Не указан';
            reportText += `Исполнитель: ${displayName}, Количество: ${row.taskCount}\n`;
        });

        ctx.reply(reportText);
    });
});



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
        nightShiftCron = cron.schedule('0 21 * * *', async () => {
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