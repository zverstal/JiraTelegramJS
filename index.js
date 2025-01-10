require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');
const cron = require('node-cron');

function getMoscowTimestamp() {
    const moscowTime = DateTime.now().setZone('Europe/Moscow');
    return moscowTime.toFormat('yyyy-MM-dd HH:mm:ss');
}

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
    taskId TEXT,
    lastCommentId TEXT,
    timestamp DATETIME,
    FOREIGN KEY(taskId) REFERENCES tasks(id)
)`);

function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

const userMappings = {
    "lipchinski": { name: "Дмитрий Селиванов", sxl: "d.selivanov", betone: "dms" },
    "pr0spal": { name: "Евгений Шушков", sxl: "e.shushkov", betone: "es" },
    "fdhsudgjdgkdfg": { name: "Даниил Маслов", sxl: "d.maslov", betone: "dam" },
    "EuroKaufman": { name: "Даниил Баратов", sxl: "d.baratov", betone: "db" },
    "Nikolay_Gonchar": { name: "Николай Гончар", sxl: "n.gonchar", betone: "ng" },
    "KIRILlKxX": { name: "Кирилл Атанизяов", sxl: "k.ataniyazov", betone: "ka" },
    "marysh353": { name: "Даниил Марышев", sxl: "d.maryshev", betone: "dma" }
};

async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira('sxl', 'https://jira.sxl.team/rest/api/2/search', process.env.JIRA_PAT_SXL, 'Техническая поддержка');
    await fetchAndStoreTasksFromJira('betone', 'https://jira.betone.team/rest/api/2/search', process.env.JIRA_PAT_BETONE, 'Техническая поддержка');
}

async function fetchAndStoreTasksFromJira(source, url, pat, ...departments) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);
        const departmentQuery = departments.map(dep => `"${dep}"`).join(" or Отдел = ");
        let jql = `project = SUPPORT AND (Отдел = ${departmentQuery}) and status = "Open"`;

        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            },
            params: { jql }
        });

        const fetchedTaskIds = response.data.issues.map(issue => issue.key);

        await new Promise((resolve, reject) => {
            if (fetchedTaskIds.length > 0) {
                const placeholders = fetchedTaskIds.map(() => '?').join(',');
                db.run(`DELETE FROM tasks WHERE id NOT IN (${placeholders}) AND source = ?`, [...fetchedTaskIds, source], function(err) {
                    if (err) reject(err);
                    else resolve();
                });
            } else {
                db.run(`DELETE FROM tasks WHERE source = ?`, [source], function(err) {
                    if (err) reject(err);
                    else resolve();
                });
            }
        });

        for (const issue of response.data.issues) {
            const task = {
                id: issue.key,
                title: issue.fields.summary,
                priority: issue.fields.priority?.name || 'Не указан',
                issueType: issue.fields.issuetype?.name || 'Не указан',
                department: issue.fields.customfield_10504?.value || 'Не указан',
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
                db.run('UPDATE tasks SET title = ?, priority = ?, issueType = ?, department = ?, source = ? WHERE id = ?', [task.title, task.priority, task.issueType, task.department, task.source, task.id]);
            } else {
                db.run('INSERT INTO tasks (id, title, priority, issueType, department, dateAdded, lastSent, source) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)', [task.id, task.title, task.priority, task.issueType, task.department, task.dateAdded, task.source]);
            }
        }
    } catch (error) {
        console.error(`Error fetching and storing tasks from ${source} Jira:`, error);
    }
}

async function sendJiraTasks(ctx) {
    const today = getMoscowTimestamp().split(' ')[0];
    const query = `
        SELECT * FROM tasks WHERE 
        (lastSent IS NULL OR lastSent < date('${today}'))
        ORDER BY priority
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
                keyboard.text('Завершить', `complete_task:${task.id}`);
                keyboard.text('Комментарий', `comment_task:${task.id}`);
            } else {
                keyboard.url('Перейти к задаче', `https://jira.${task.source === 'sxl' ? 'sxl' : 'betone'}.team/browse/${task.id}`);
            }

            const messageText = `Задача: ${task.id}\nИсточник: ${task.source}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nТип задачи: ${task.issueType}`;

            await ctx.reply(messageText, { reply_markup: keyboard });

            const moscowTimestamp = getMoscowTimestamp();
            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [moscowTimestamp, task.id]);
        }
    });
}

bot.callbackQuery(/^comment_task:(.+)$/, async (ctx) => {
    const taskId = ctx.match[1];
    await ctx.reply(`Введите ваш комментарий для задачи ${taskId}:`);

    bot.on('message:text', async (messageCtx) => {
        const comment = messageCtx.message.text;
        try {
            const task = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM tasks WHERE id = ?', [taskId], (err, task) => {
                    if (err) reject(err);
                    else resolve(task);
                });
            });

            const url = `https://jira.${task.source}.team/rest/api/2/issue/${taskId}/comment`;
            await axios.post(url, {
                body: comment
            }, {
                headers: {
                    'Authorization': `Bearer ${task.source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE}`,
                    'Content-Type': 'application/json'
                }
            });

            await ctx.reply(`Комментарий добавлен: "${comment}"`);
        } catch (error) {
            console.error('Ошибка при добавлении комментария в Jira:', error);
            await ctx.reply('Не удалось добавить комментарий. Попробуйте позже.');
        }
    });
});

async function checkNewComments() {
    try {
        const url = `https://jira.sxl.team/rest/api/2/search`;
        const jql = `resolution = Done AND updated >= -30d`;
        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${process.env.JIRA_PAT_SXL}`,
                'Accept': 'application/json'
            },
            params: { jql }
        });

        for (const issue of response.data.issues) {
            const taskId = issue.key;
            const commentsUrl = `https://jira.sxl.team/rest/api/2/issue/${taskId}/comment`;
            const commentsResponse = await axios.get(commentsUrl, {
                headers: {
                    'Authorization': `Bearer ${process.env.JIRA_PAT_SXL}`,
                    'Accept': 'application/json'
                }
            });

            const comments = commentsResponse.data.comments;
            const lastComment = comments[comments.length - 1];

            const storedComment = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM task_comments WHERE taskId = ?', [taskId], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            if (!storedComment || storedComment.lastCommentId !== lastComment.id) {
                db.run('REPLACE INTO task_comments (taskId, lastCommentId, timestamp) VALUES (?, ?, ?)', [taskId, lastComment.id, getMoscowTimestamp()]);

                const messageText = `Задача: ${taskId}\nИсточник: SXL\nОписание: ${issue.fields.summary}\nПриоритет: ${getPriorityEmoji(issue.fields.priority?.name)}\nТип задачи: ${issue.fields.issuetype?.name}\nКомментарий: "${lastComment.body}"\nАвтор: ${lastComment.author.displayName}`;
                await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, messageText);
            }
        }
    } catch (error) {
        console.error('Ошибка при проверке новых комментариев:', error);
    }
}

cron.schedule('*/30 * * * *', checkNewComments);

bot.command('report', async (ctx) => {
    const today = getMoscowTimestamp().split(' ')[0];
    const lastWeek = DateTime.now().minus({ days: 7 }).toFormat('yyyy-MM-dd');

    const query = `
        SELECT * FROM user_actions 
        WHERE action = "complete" 
        AND timestamp BETWEEN '${lastWeek}' AND '${today}'
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('Ошибка при создании отчета:', err);
            await ctx.reply('Не удалось создать отчет. Попробуйте позже.');
            return;
        }

        if (rows.length === 0) {
            await ctx.reply('За указанный период нет завершенных задач.');
            return;
        }

        let report = 'Отчет о завершенных задачах за неделю:\n';
        rows.forEach(row => {
            report += `Задача: ${row.taskId}, Пользователь: ${row.username}, Дата завершения: ${row.timestamp}\n`;
        });

        await ctx.reply(report);
    });
});

cron.schedule('0 21 * * *', async () => {
    console.log('Night shift reminder sent.');
    await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброй ночи! Заполни тикет передачи смены.');
});

cron.schedule('0 9 * * *', async () => {
    console.log('Morning reminder sent.');
    await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброе утро! Проверь задачи на сегодня и начни смену.');
});

bot.command('start', async (ctx) => {
    await ctx.reply('Привет! Я буду сообщать о новых задачах и уведомлять о комментариях. Используйте команды: /report для отчёта по выполненным задачам.');
    fetchAndStoreJiraTasks();
    sendJiraTasks(ctx);

    cron.schedule('*/10 * * * *', async () => {
        console.log('Checking for new tasks...');
        await fetchAndStoreJiraTasks();
        await sendJiraTasks(ctx);
    });
});

bot.start();