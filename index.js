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
    dateAdded DATETIME,
    lastSent DATETIME,
    source TEXT -- Источник задачи
)`);

db.run(`CREATE TABLE IF NOT EXISTS user_actions (
    username TEXT,
    taskId TEXT,
    action TEXT,
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

function sendNightShiftMessage(ctx) {
    ctx.reply('Ночной дозор! Начни смену в боте в 21:00 https://t.me/NightShiftBot_bot');
}

async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira('sxl', 'https://jira.sxl.team/rest/api/2/search', process.env.JIRA_PAT_SXL, 'QA', 'Sportsbook','Техническая поддержка');
    await fetchAndStoreTasksFromJira('betone', 'https://jira.betone.team/rest/api/2/search', process.env.JIRA_PAT_BETONE, 'QA', 'Техническая поддержка');
}

async function fetchAndStoreTasksFromJira(source, url, pat, ...departments) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);
        const departmentQuery = departments.map(dep => `"${dep}"`).join(" or Отдел = ");
        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            },
            params: {
                jql: `project = SUPPORT AND (Отдел = ${departmentQuery}) and status = "Open"`
            }
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
                priority: issue.fields.priority.name,
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
                db.run('UPDATE tasks SET title = ?, priority = ?, department = ?, source = ? WHERE id = ?', [task.title, task.priority, task.department, task.source, task.id]);
            } else {
                db.run('INSERT INTO tasks (id, title, priority, department, dateAdded, lastSent, source) VALUES (?, ?, ?, ?, ?, NULL, ?)', [task.id, task.title, task.priority, task.department, task.dateAdded, task.source]);
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
        (department IN ("QA", "Sportsbook") AND (lastSent IS NULL OR lastSent < datetime('now', '-3 days')))
        OR
        (department = "Техническая поддержка" AND (lastSent IS NULL OR lastSent < date('${today}')))
        ORDER BY CASE 
            WHEN department = 'QA' THEN 1 
            WHEN department = 'Sportsbook' THEN 2 
            ELSE 3 
        END
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('Error fetching tasks:', err);
            return;
        }

        for (const task of rows) {
            const department = task.department;
            const keyboard = new InlineKeyboard();

            if (department === "Техническая поддержка") {
                keyboard.text('Взять в работу', `take_task:${task.id}`);
            } else if (department === "QA" || department === "Sportsbook") {
                keyboard.text('В курсе', `aware_task:${task.id}`);
            }

            const messageText = `Задача: ${task.id}\nИсточник: ${task.source}\nСсылка: https://jira.${task.source === 'sxl' ? 'sxl' : 'betone'}.team/browse/${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${department}`;
            console.log('Sending message to Telegram:', messageText);

            await ctx.reply(messageText, { reply_markup: keyboard });

            const moscowTimestamp = getMoscowTimestamp();
            console.log(`Updating lastSent for task ${task.id} to: ${moscowTimestamp}`);
            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [moscowTimestamp, task.id]);
        }
    });
}

const usernameMappings = {
    "lipchinski": "Дмитрий Селиванов",
    "ayugoncharov": "Александр Гончаров",
    "fdhsudgjdgkdfg": "Даниил Маслов",
    "EuroKaufman": "Даниил Баратов",
    "Nikolay_Gonchar": "Николай Гончар",
    "KIRILlKxX": "Кирилл Атанизяов"
};

const jiraUserMappings = {
    "lipchinski": { "sxl": "d.selivanov", "betone": "dms" },
    "ayugoncharov": { "sxl": "a.goncharov", "betone": "ag" },
    "fdhsudgjdgkdfg": { "sxl": "d.maslov", "betone": "dam" },
    "EuroKaufman": { "sxl": "d.baratov", "betone": "db" },
    "Nikolay_Gonchar": { "sxl": "n.gonchar", "betone": "ng" },
    "KIRILlKxX": { "sxl": "k.ataniyazov", "betone": "ka" }
};

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
                    const messageText = `Задача: ${task.id}\nИсточник: ${task.source}\nСсылка: https://jira.${task.source === 'sxl' ? 'sxl' : 'betone'}.team/browse/${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${task.department}\n\nВзял в работу: ${displayName}`;

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

bot.callbackQuery(/^aware_task:(.+)$/, async (ctx) => {
    try {
        const taskId = ctx.match[1];
        const username = ctx.from.username;

        db.get('SELECT * FROM tasks WHERE id = ?', [taskId], async (taskErr, task) => {
            if (taskErr) {
                console.error('Ошибка при получении информации о задаче:', taskErr);
                await ctx.reply('Произошла ошибка при обработке вашего запроса.');
                return;
            }

            if (!task) {
                await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
                await ctx.reply('Задача не найдена.');
                return;
            }

            db.get('SELECT * FROM user_actions WHERE username = ? AND taskId = ? AND action = "aware_task"', [username, taskId], async (err, row) => {
                if (err) {
                    console.error('Ошибка при запросе к базе данных:', err);
                    return;
                }

                if (!row) {
                    await db.run('INSERT OR IGNORE INTO user_actions (username, taskId, action, timestamp) VALUES (?, ?, ?, ?)', [username, taskId, 'aware_task', getMoscowTimestamp()]);
                } else {
                    ctx.answerCallbackQuery('Вы уже отметили эту задачу как просмотренную.');
                }

                db.all('SELECT DISTINCT username FROM user_actions WHERE taskId = ? AND action = "aware_task"', [taskId], async (selectErr, users) => {
                    if (selectErr) {
                        console.error('Ошибка при получении списка пользователей:', selectErr);
                        return;
                    }

                    const awareUsersList = users.map(u => usernameMappings[u.username] || u.username).join(', ');
                    const lastUpdated = new Date().toLocaleTimeString();
                    const messageText = `Задача: ${task.id}\nИсточник: ${task.source}\nСсылка: https://jira.${task.source === 'sxl' ? 'sxl' : 'betone'}.team/browse/${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${task.department}\n\nПользователи в курсе задачи: ${awareUsersList}\n\nПоследнее обновление: ${lastUpdated}`;
                    const replyMarkup = users.length >= 3 ? undefined : ctx.callbackQuery.message.reply_markup;

                    await ctx.editMessageText(messageText, { reply_markup: replyMarkup });
                });
            });
        });
    } catch (error) {
        console.error('Ошибка в обработчике aware_task:', error);
        await ctx.reply('Произошла ошибка при обработке вашего запроса.');
    }
});

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

        const jiraUsername = jiraUserMappings[telegramUsername][source];
        if (!jiraUsername) {
            console.error(`No Jira username mapping found for Telegram username: ${telegramUsername}`);
            return false;
        }

        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}/assignee`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;
        
        const assigneeResponse = await axios.put(url, {
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
        console.error(`Error updating ${source} Jira task:`, error);
        return false;
    }
}

let interval;
let nightShiftCron;
let morningShiftCron;

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
            await ctx.reply('Доброй ночи! Заполни тикет передачи смены и внеси дела для утренней смены сюда: https://plan-kaban.ru/boards/1207384783689090054');

            if (!morningShiftCron) {
                morningShiftCron = cron.schedule('0 10 * * *', async () => {
                    await ctx.reply('Доброе утро! Не забудь проверить задачи на сегодня: https://plan-kaban.ru/boards/1207384783689090054');
                }, {
                    scheduled: false,
                    timezone: "Europe/Moscow"
                });

                morningShiftCron.start();
            }
        }, {
            scheduled: false,
            timezone: "Europe/Moscow"
        });

        nightShiftCron.start();
    }
});

bot.command('stop', async (ctx) => {
    if (interval) {
        clearInterval(interval);
        interval = null;
        await ctx.reply('Интервал остановлен.');
    } else {
        await ctx.reply('Интервал не был запущен.');
    }
});

bot.start();
