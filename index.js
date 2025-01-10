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
            jql = `project = SUPPORT AND (Отдел = ${departmentQuery}) and status = "Open"`;
        }

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
            } else if (["Infra", "Office", "Prod"].includes(task.issueType) && ["Open", "Under review", "Waiting for support", "Waiting for Developers approval"].includes(task.status)) {
                keyboard.url('Перейти к задаче', `https://jira.${task.source === 'sxl' ? 'sxl' : 'betone'}.team/browse/${task.id}`);
            }

            const messageText = `Задача: ${task.id}\nИсточник: ${task.source}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nТип задачи: ${task.issueType}`;

            await ctx.reply(messageText, { reply_markup: keyboard });

            const moscowTimestamp = getMoscowTimestamp();
            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [moscowTimestamp, task.id]);
        }
    });
}

bot.command('start', async (ctx) => {
    await ctx.reply('Привет! Я буду сообщать о новых задачах и уведомлять о комментариях. Используйте команды:

/report - Отчёт по выполненным задачам.');
    fetchAndStoreJiraTasks();
    sendJiraTasks(ctx);

    cron.schedule('*/1 * * * *', async () => {
        console.log('Checking for new tasks...');
        await fetchAndStoreJiraTasks();
        await sendJiraTasks(ctx);
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

bot.start();