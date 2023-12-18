require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon'); // Подключаем Luxon для работы с временем

const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');

function getMoscowTimestamp() {
    // Получаем текущее время и дату
    const moscowTime = DateTime.now().setZone('Europe/Moscow');

    // Форматируем время в строку
    return moscowTime.toFormat('yyyy-MM-dd HH:mm:ss');
}

db.run(`CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT,
    priority TEXT,
    department TEXT,
    dateAdded DATETIME,     -- Дата добавления задачи
    lastSent DATETIME       -- Дата отправки задачи
)`);

db.run(`CREATE TABLE IF NOT EXISTS user_actions (
    username TEXT,
    taskId TEXT,
    action TEXT,
    timestamp DATETIME,  -- Просто DATETIME
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

async function fetchAndStoreJiraTasks() {
    try {
        console.log('Fetching tasks from Jira...');
        const response = await axios.get('https://jira.sxl.team/rest/api/2/search', {
            headers: {
                'Authorization': `Bearer ${process.env.JIRA_PAT}`,
                'Accept': 'application/json'
            },
            params: {
                jql: 'project = SUPPORT AND (Отдел = "Техническая поддержка" or Отдел = "QA" or Отдел = "Sportsbook") and status = "Open"'
            }
        });
        console.log('Jira API response:', response.data);

        const fetchedTaskIds = response.data.issues.map(issue => issue.key);

        // Удаление задач из базы данных, которые больше не "Open" в Jira
        await new Promise((resolve, reject) => {
            const placeholders = fetchedTaskIds.map(() => '?').join(',');
            db.run(`DELETE FROM tasks WHERE id NOT IN (${placeholders})`, fetchedTaskIds, function(err) {
                if (err) {
                    reject(err);
                    console.error('Error deleting tasks:', err);
                } else {
                    resolve();
                }
            });
        });

        // Добавление или обновление задач в базе данных
        for (const issue of response.data.issues) {
            const task = {
                id: issue.key,
                title: issue.fields.summary,
                priority: issue.fields.priority.name,
                department: issue.fields.customfield_10500 ? issue.fields.customfield_10500.value : 'Не указан',
                dateAdded: getMoscowTimestamp()
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
                db.run('UPDATE tasks SET title = ?, priority = ?, department = ? WHERE id = ?', [task.title, task.priority, task.department, task.id]);
            } else {
                db.run('INSERT INTO tasks (id, title, priority, department, dateAdded, lastSent) VALUES (?, ?, ?, ?, ?, NULL)', [task.id, task.title, task.priority, task.department, task.dateAdded]);
            }
        }
    } catch (error) {
        console.error('Error fetching and storing Jira tasks:', error);
    }
}



async function sendJiraTasks(ctx) {
    // Получаем текущую дату в формате ГГГГ-ММ-ДД
    const today = getMoscowTimestamp().split(' ')[0];

    // Формируем SQL-запрос с учетом условий отправки и сортировкой по отделу
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

            // Собираем клавиатуру в зависимости от отдела
            if (department === "Техническая поддержка") {
                keyboard.text('Взять в работу', `take_task:${task.id}`);
            } else if (department === "QA" || department === "Sportsbook") {
                keyboard.text('В курсе', `aware_task:${task.id}`);
            }

            const messageText = `Задача: ${task.id}\nСсылка: https://jira.sxl.team/browse/${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${department}`;
            console.log('Sending message to Telegram:', messageText);

            await ctx.reply(messageText, { reply_markup: keyboard });

            // Обновляем lastSent в базе данных для задачи
            const moscowTimestamp = getMoscowTimestamp();
            console.log(`Updating lastSent for task ${task.id} to: ${moscowTimestamp}`);
            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [moscowTimestamp, task.id]);
        }
    });
}


const usernameMappings = {
    "lipchinski": "Дмитрий Селиванов",
    "YurkovOfficial": "Пётр Юрков",
    "Jlufi": "Даниил Маслов",
    "EuroKaufman": "Даниил Баратов"
};

bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
    try {
        const taskId = ctx.match[1];

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
                const success = await updateJiraTaskStatus(taskId);
                if (success) {
                    const displayName = usernameMappings[ctx.from.username] || ctx.from.username;

                    // Формируем текст сообщения с полными деталями задачи
                    const messageText = `Задача: ${task.id}\nСсылка: https://jira.sxl.team/browse/${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${task.department}\n\nВзял в работу: ${displayName}`;

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

        // Получаем информацию о задаче
        db.get('SELECT * FROM tasks WHERE id = ?', [taskId], async (taskErr, task) => {
            if (taskErr) {
                console.error('Ошибка при получении информации о задаче:', taskErr);
                await ctx.reply('Произошла ошибка при обработке вашего запроса.');
                return;
            }

            if (!task) {
                // Задача не найдена, удаляем клавиатуру
                await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
                await ctx.reply('Задача не найдена.');
                return;
            }

            // Проверяем, отмечал ли этот пользователь задачу как 'в курсе' ранее
            db.get('SELECT * FROM user_actions WHERE username = ? AND taskId = ? AND action = "aware_task"', [username, taskId], async (err, row) => {
                if (err) {
                    console.error('Ошибка при запросе к базе данных:', err);
                    return;
                }

                if (!row) {
                    // Если пользователь ранее не отмечал задачу, добавляем запись в базу данных
                    db.run('INSERT INTO user_actions (username, taskId, action, timestamp) VALUES (?, ?, ?, ?)', [username, taskId, 'aware_task', getMoscowTimestamp()]);
                }

                // Обновляем список пользователей, осведомленных о задаче
                db.all('SELECT DISTINCT username FROM user_actions WHERE taskId = ? AND action = "aware_task"', [taskId], async (selectErr, awareUsers) => {
                    if (selectErr) {
                        console.error('Ошибка при получении списка пользователей:', selectErr);
                        return;
                    }

                    const awareUsersList = awareUsers.map(row => usernameMappings[row.username] || row.username).join(', ');
                    const messageText = `Задача: ${task.id}\nСсылка: https://jira.sxl.team/browse/${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${task.department}\n\nПользователи в курсе задачи: ${awareUsersList}`;

                    const replyMarkup = awareUsers.length >= 3 ? undefined : ctx.callbackQuery.message.reply_markup;
                    await ctx.editMessageText(messageText, { reply_markup: replyMarkup });
                });
            });
        });
    } catch (error) {
        console.error('Ошибка в обработчике aware_task:', error);
        await ctx.reply('Произошла ошибка при обработке вашего запроса.');
    }
});



async function updateJiraTaskStatus(taskId) {
    try {
        const transitionId = '221'; // Замените на актуальный ID
        const transitionResponse = await axios.post(`https://jira.sxl.team/rest/api/2/issue/${taskId}/transitions`, {
            transition: {
                id: transitionId
            }
        }, {
            headers: {
                'Authorization': `Bearer ${process.env.JIRA_PAT}`,
                'Content-Type': 'application/json'
            }
        });
        return transitionResponse.status === 204;
    } catch (error) {
        console.error('Error updating Jira task:', error);
        return false;
    }
}

let interval;

bot.command('start', async (ctx) => {
    await ctx.reply('Привет! Каждую минуту я буду проверять новые задачи...');

    if (!interval) {
        interval = setInterval(async () => {
            console.log('Interval triggered. Sending Jira tasks...');
            await fetchAndStoreJiraTasks(); // Обновление задач
            await sendJiraTasks(ctx); // Отправка задач текущему пользователю
            console.log('Jira tasks sent.');
        }, 60000);  // 60000 миллисекунд = 1 минута
    } else {
        await ctx.reply('Интервал уже запущен.');
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
