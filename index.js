require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');
const cron = require('node-cron');

/**
 * Возвращает дату-время по Москве в формате 'yyyy-MM-dd HH:mm:ss'.
 */
function getMoscowTimestamp() {
    const moscowTime = DateTime.now().setZone('Europe/Moscow');
    return moscowTime.toFormat('yyyy-MM-dd HH:mm:ss');
}

/**
 * Создаём (или проверяем существование) таблиц в БД.
 * Добавлены поля archived, archivedDate в tasks.
 */
db.run(`
  CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT,
    priority TEXT,
    department TEXT,
    issueType TEXT,
    resolution TEXT,
    assignee TEXT,
    dateAdded DATETIME,
    lastSent DATETIME,
    source TEXT,
    archived INTEGER DEFAULT 0,
    archivedDate DATETIME
  )
`);

db.run(`
  CREATE TABLE IF NOT EXISTS user_actions (
    username TEXT,
    taskId TEXT,
    action TEXT,
    timestamp DATETIME,
    FOREIGN KEY(taskId) REFERENCES tasks(id)
)
`);

db.run(`
  CREATE TABLE IF NOT EXISTS task_comments (
    taskId TEXT,
    lastCommentId TEXT,
    timestamp DATETIME,
    FOREIGN KEY(taskId) REFERENCES tasks(id)
)
`);

/**
 * Карта пользователей (Telegram ник -> ФИО), плюс логины в Jira.
 */
const userMappings = {
    "lipchinski": {
        name: "Дмитрий Селиванов",
        sxl: "d.selivanov",
        betone: "dms"
    },
    "pr0spal": {
        name: "Евгений Шушков",
        sxl: "e.shushkov",
        betone: "es"
    },
    "fdhsudgjdgkdfg": {
        name: "Даниил Маслов",
        sxl: "d.maslov",
        betone: "dam"
    },
    "EuroKaufman": {
        name: "Даниил Баратов",
        sxl: "d.baratov",
        betone: "db"
    },
    "Nikolay_Gonchar": {
        name: "Николай Гончар",
        sxl: "n.gonchar",
        betone: "ng"
    },
    "KIRILlKxX": {
        name: "Кирилл Атанизяов",
        sxl: "k.ataniyazov",
        betone: "ka"
    },
    "marysh353": {
        name: "Даниил Марышев",
        sxl: "d.maryshev",
        betone: "dma"
    }
};

/**
 * Мапим assignee из Jira (например, d.selivanov / dms) на ФИО.
 */
function mapAssigneeToName(assigneeFromJira) {
    for (const key in userMappings) {
        const mapObj = userMappings[key];
        if (mapObj.sxl === assigneeFromJira || mapObj.betone === assigneeFromJira) {
            return mapObj.name;
        }
    }
    return ''; // Не нашли
}

/**
 * Мапим Telegram username (например, lipchinski) -> ФИО ("Дмитрий Селиванов").
 */
function mapTelegramUserToName(tlgUsername) {
    if (!tlgUsername) return 'Неизвестный пользователь';
    if (userMappings[tlgUsername]) {
        return userMappings[tlgUsername].name;
    }
    return 'Неизвестный пользователь';
}

/**
 * Эмоджи для приоритета.
 */
function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

/**
 * Основная функция, которая ходит в 2 источника (sxl, betone) за задачами.
 */
async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira(
        'sxl',
        'https://jira.sxl.team/rest/api/2/search',
        process.env.JIRA_PAT_SXL
    );
    await fetchAndStoreTasksFromJira(
        'betone',
        'https://jira.betone.team/rest/api/2/search',
        process.env.JIRA_PAT_BETONE
    );
}

/**
 * Получаем актуальные задачи (Open, Under review, Waiting..., Done),
 * ставим archived=0 для пришедших.
 * Остальные (не пришедшие) – помечаем archived=1, archivedDate=now.
 *
 * ВАЖНО: Исправили логику определения поля department:
 *  - Для 'sxl' берём fields.customfield_10500?.value
 *  - Для 'betone' берём fields.customfield_10504?.value
 *  - По необходимости замените поля на те, которые реально используются в ваших Jira.
 */
async function fetchAndStoreTasksFromJira(source, url, pat) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);

        const jql = `
            project = SUPPORT
            AND status in ("Open", "Under review", "Waiting for support", "Waiting for Developers approval", "Done")
        `;
        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            },
            params: { jql }
        });

        const fetchedIssues = response.data.issues || [];
        const fetchedTaskIds = fetchedIssues.map(issue => issue.key);

        // Обновляем / вставляем задачи из Jira
        for (const issue of fetchedIssues) {
            const fields = issue.fields;

            // Логика определения department в зависимости от source
            let department = 'Не указан';
            if (source === 'sxl') {
                // Например, для SXL используем customfield_10500
                department = fields.customfield_10500?.value || 'Не указан';
            } else if (source === 'betone') {
                // Для BetOne используем customfield_10504
                department = fields.customfield_10504?.value || 'Не указан';
            }

            const resolution = fields.resolution?.name || '';
            const assigneeKey = fields.assignee?.name || '';  // например "d.selivanov"
            const assigneeName = mapAssigneeToName(assigneeKey);

            const taskData = {
                id: issue.key,
                title: fields.summary,
                priority: fields.priority?.name || 'Не указан',
                issueType: fields.issuetype?.name || 'Не указан',
                department,
                resolution,
                assignee: assigneeName,
                source
            };

            // Проверяем, есть ли задача в БД
            const existingTask = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM tasks WHERE id = ?', [taskData.id], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            if (existingTask) {
                // UPDATE
                db.run(
                    `UPDATE tasks
                     SET title = ?,
                         priority = ?,
                         issueType = ?,
                         department = ?,
                         resolution = ?,
                         assignee = ?,
                         source = ?,
                         archived = 0 -- снимаем флаг архивирования
                     WHERE id = ?`,
                    [
                        taskData.title,
                        taskData.priority,
                        taskData.issueType,
                        taskData.department,
                        taskData.resolution,
                        taskData.assignee,
                        taskData.source,
                        taskData.id
                    ]
                );
            } else {
                // INSERT
                db.run(
                    `INSERT INTO tasks
                     (id, title, priority, issueType, department, resolution, assignee, dateAdded, lastSent, source, archived, archivedDate)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 0, NULL)`,
                    [
                        taskData.id,
                        taskData.title,
                        taskData.priority,
                        taskData.issueType,
                        taskData.department,
                        taskData.resolution,
                        taskData.assignee,
                        getMoscowTimestamp(),
                        taskData.source
                    ]
                );
            }
        }

        // Задачи, которые НЕ пришли из Jira (не в fetchedTaskIds) — архивируем
        if (fetchedTaskIds.length > 0) {
            const placeholders = fetchedTaskIds.map(() => '?').join(',');
            db.run(
                `UPDATE tasks
                 SET archived = 1,
                     archivedDate = ?
                 WHERE source = ?
                   AND archived = 0
                   AND id NOT IN (${placeholders})`,
                [getMoscowTimestamp(), source, ...fetchedTaskIds],
                function(err) {
                    if (err) {
                        console.error('Error archiving tasks:', err);
                    } else {
                        if (this.changes > 0) {
                            console.log(`Archived ${this.changes} tasks from ${source}.`);
                        }
                    }
                }
            );
        } else {
            // Если вообще нет задач, всё (этого source) архивируем
            db.run(
                `UPDATE tasks
                 SET archived = 1,
                     archivedDate = ?
                 WHERE source = ?
                   AND archived = 0`,
                [getMoscowTimestamp(), source],
                function(err) {
                    if (err) {
                        console.error('Error archiving tasks (none fetched):', err);
                    } else {
                        if (this.changes > 0) {
                            console.log(`Archived ${this.changes} tasks from ${source} (none fetched).`);
                        }
                    }
                }
            );
        }
    } catch (error) {
        console.error(`Error fetching and storing tasks from ${source} Jira:`, error);
    }
}

/**
 * Рассылаем "свежие" задачи (не Done, archived=0), где lastSent < сегодня.
 */
async function sendJiraTasks(ctx) {
    const today = getMoscowTimestamp().split(' ')[0];
    const query = `
        SELECT *
        FROM tasks
        WHERE (lastSent IS NULL OR lastSent < date('${today}'))
          AND resolution != 'Done'
          AND archived = 0
        ORDER BY priority
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('Error fetching tasks:', err);
            return;
        }

        for (const task of rows) {
            const keyboard = new InlineKeyboard();
            const jiraUrl = `https://jira.${task.source}.team/browse/${task.id}`;

            // Проверяем: department === 'Техническая поддержка'
            if (task.department === 'Техническая поддержка') {
                keyboard
                    .text('Взять в работу', `take_task:${task.id}`)
                    .text('Комментарий', `comment_task:${task.id}`)
                    .text('Завершить', `complete_task:${task.id}`)
                    .row()
                    .url('Перейти к задаче', jiraUrl);
            } else if (['Infra', 'Office', 'Prod'].includes(task.issueType)) {
                keyboard.url('Перейти к задаче', jiraUrl);
            } else {
                keyboard.url('Перейти к задаче', jiraUrl);
            }

            const messageText = `
Задача: ${task.id}
Источник: ${task.source}
Описание: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}
Исполнитель: ${task.assignee || 'не назначен'}
            `.trim();

            await ctx.reply(messageText, {
                reply_markup: keyboard
            });

            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [getMoscowTimestamp(), task.id]);
        }
    });
}

/**
 * Ежедневная очистка (например, в полночь):
 * удаляем из БД задачи, которые действительно не нужны, 
 * например, archived=1 и resolution='Done' старше 35 дней.
 */
cron.schedule('0 0 * * *', () => {
    console.log('Starting daily DB cleanup...');

    const cleanupQuery = `
        DELETE FROM tasks
        WHERE archived = 1
          AND resolution = 'Done'
          AND archivedDate IS NOT NULL
          AND date(archivedDate) < date('now','-35 days')
    `;
    db.run(cleanupQuery, function(err) {
        if (err) {
            console.error('Error cleaning up archived tasks:', err);
        } else {
            console.log(`Cleaned up ${this.changes} old archived tasks (Done).`);
        }
    });

    // Чистим user_actions, если task уже нет
    const cleanupUserActions = `
        DELETE FROM user_actions
        WHERE taskId NOT IN (SELECT id FROM tasks)
    `;
    db.run(cleanupUserActions, function(err) {
        if (err) {
            console.error('Error cleaning up old user_actions:', err);
        } else {
            console.log(`Cleaned up ${this.changes} old user_actions.`);
        }
    });

    // Чистим task_comments, если task уже нет
    const cleanupComments = `
        DELETE FROM task_comments
        WHERE taskId NOT IN (SELECT id FROM tasks)
    `;
    db.run(cleanupComments, function(err) {
        if (err) {
            console.error('Error cleaning up old task_comments:', err);
        } else {
            console.log(`Cleaned up ${this.changes} old task_comments.`);
        }
    });
});

/**
 * Проверяем каждые 5 минут новые комментарии в задачах Done (Техподдержка).
 */
async function checkNewCommentsInDoneTasks() {
    try {
        const query = `
            SELECT *
            FROM tasks
            WHERE department = 'Техническая поддержка'
              AND resolution = 'Done'
              AND archived = 0
        `;

        db.all(query, [], async (err, tasks) => {
            if (err) {
                console.error('Error fetching done tasks for comments check:', err);
                return;
            }

            for (const task of tasks) {
                const tableCommentInfo = await new Promise((resolve, reject) => {
                    db.get(
                        `SELECT * FROM task_comments WHERE taskId = ?`,
                        [task.id],
                        (err2, row) => {
                            if (err2) reject(err2);
                            else resolve(row);
                        }
                    );
                });

                const lastSavedCommentId = tableCommentInfo ? tableCommentInfo.lastCommentId : null;

                // Запрашиваем комментарии
                const commentUrl = `https://jira.${task.source}.team/rest/api/2/issue/${task.id}/comment`;
                const response = await axios.get(commentUrl, {
                    headers: {
                        Authorization: `Bearer ${
                            task.source === 'sxl'
                                ? process.env.JIRA_PAT_SXL
                                : process.env.JIRA_PAT_BETONE
                        }`,
                        Accept: 'application/json'
                    }
                });

                const allComments = response.data.comments || [];
                // Сортируем по ID (если он числовой)
                allComments.sort((a, b) => parseInt(a.id) - parseInt(b.id));

                let newLastId = lastSavedCommentId;
                for (const comment of allComments) {
                    const commentIdNum = parseInt(comment.id);
                    const lastSavedIdNum = lastSavedCommentId ? parseInt(lastSavedCommentId) : 0;

                    if (commentIdNum > lastSavedIdNum) {
                        // Новый комментарий
                        const authorName = comment.author?.displayName || 'Неизвестный автор';
                        const bodyText = comment.body || '';

                        const messageText = `
В выполненную задачу добавлен новый комментарий

Задача: ${task.id}
Источник: ${task.source}
Описание: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}

Автор комментария: ${authorName}
Комментарий: ${bodyText}
                        `.trim();

                        await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, messageText);

                        if (!newLastId || commentIdNum > parseInt(newLastId)) {
                            newLastId = comment.id;
                        }
                    }
                }

                // Обновляем запись в БД
                if (newLastId && newLastId !== lastSavedCommentId) {
                    if (tableCommentInfo) {
                        db.run(
                            `UPDATE task_comments
                             SET lastCommentId = ?, timestamp = ?
                             WHERE taskId = ?`,
                            [newLastId, getMoscowTimestamp(), task.id]
                        );
                    } else {
                        db.run(
                            `INSERT INTO task_comments (taskId, lastCommentId, timestamp)
                             VALUES (?, ?, ?)`,
                            [task.id, newLastId, getMoscowTimestamp()]
                        );
                    }
                }
            }
        });
    } catch (error) {
        console.error('checkNewCommentsInDoneTasks error:', error);
    }
}

/**
 * /report — статистика по выполненным задачам (Done, Техподдержка) за 30 дней.
 * Учитываются и архивные, и неархивные. 
 */
bot.command('report', async (ctx) => {
    try {
        const thirtyDaysAgo = DateTime.now().setZone('Europe/Moscow')
            .minus({ days: 30 })
            .toFormat('yyyy-MM-dd');

        const query = `
            SELECT assignee
            FROM tasks
            WHERE resolution = 'Done'
              AND department = 'Техническая поддержка'
              AND date(dateAdded) >= date(?)
        `;

        db.all(query, [thirtyDaysAgo], async (err, rows) => {
            if (err) {
                console.error('/report error:', err);
                await ctx.reply('Произошла ошибка при формировании отчёта.');
                return;
            }

            if (!rows || rows.length === 0) {
                await ctx.reply('За последние 30 дней нет выполненных задач в Техподдержке.');
                return;
            }

            const stats = {};
            for (const row of rows) {
                const name = row.assignee || 'Неизвестный'; 
                if (!stats[name]) stats[name] = 0;
                stats[name]++;
            }

            let reportMessage = 'Отчёт по выполненным задачам (Техподдержка) за последние 30 дней:\n\n';
            for (const name of Object.keys(stats)) {
                reportMessage += `${name}: ${stats[name]} задач(и)\n`;
            }

            await ctx.reply(reportMessage);
        });
    } catch (error) {
        console.error('Error in /report command:', error);
        await ctx.reply('Произошла ошибка при формировании отчёта.');
    }
});

/**
 * Обработчики инлайн-кнопок (take_task, comment_task, complete_task).
 * username в БД теперь храним как ФИО, а не Telegram-ник.
 */
bot.callbackQuery(/^(take_task|comment_task|complete_task):(.*)$/, async (ctx) => {
    const actionType = ctx.match[1]; // take_task | comment_task | complete_task
    const taskId = ctx.match[2];

    // Мапим Telegram username -> ФИО
    const realName = mapTelegramUserToName(ctx.from?.username);

    db.run(
        `INSERT INTO user_actions (username, taskId, action, timestamp)
         VALUES (?, ?, ?, ?)`,
        [realName, taskId, actionType, getMoscowTimestamp()],
        (err) => {
            if (err) {
                console.error('Error saving user action:', err);
            }
        }
    );

    let replyText;
    switch (actionType) {
        case 'take_task':
            replyText = `${realName} взял(а) задачу ${taskId} в работу.`;
            break;
        case 'comment_task':
            replyText = `${realName} хочет добавить комментарий к задаче ${taskId}.`;
            break;
        case 'complete_task':
            replyText = `${realName} завершил(а) задачу ${taskId}.`;
            break;
        default:
            replyText = `${realName} сделал(а) действие: ${actionType}.`;
            break;
    }

    await ctx.answerCallbackQuery(); // убираем "loading..."
    await ctx.reply(replyText);
});

/**
 * /start — приветствие, запуск бота, настройка cron-задач.
 */
bot.command('start', async (ctx) => {
    await ctx.reply(
        'Привет! Я буду сообщать о новых задачах и уведомлять о комментариях.\n' +
        'Используйте команды:\n' +
        '/report - Показать статистику по выполненным задачам за последние 30 дней (Техподдержка).'
    );

    // Сразу при старте загрузим задачи
    fetchAndStoreJiraTasks().then(() => sendJiraTasks(ctx));

    // Проверяем новые задачи каждые 2 минуты
    cron.schedule('*/2 * * * *', async () => {
        console.log('Checking for new/updated tasks...');
        await fetchAndStoreJiraTasks();
        // при желании можно реже слать sendJiraTasks
        await sendJiraTasks(ctx);
    });
});

/**
 * Каждые 5 минут — проверяем новые комментарии в закрытых задачах Техподдержки
 */
cron.schedule('*/5 * * * *', async () => {
    console.log('Checking new comments in done tasks...');
    await checkNewCommentsInDoneTasks();
});

/**
 * Пример уведомлений в 21:00 и 09:00
 */
cron.schedule('0 21 * * *', async () => {
    console.log('Night shift reminder sent.');
    await bot.api.sendMessage(
        process.env.ADMIN_CHAT_ID,
        'Доброй ночи! Заполни тикет передачи смены.'
    );
});
cron.schedule('0 9 * * *', async () => {
    console.log('Morning reminder sent.');
    await bot.api.sendMessage(
        process.env.ADMIN_CHAT_ID,
        'Доброе утро! Проверь задачи на сегодня и начни смену.'
    );
});

/**
 * Запуск бота
 */
bot.start();
