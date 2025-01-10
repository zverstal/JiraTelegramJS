// index.js

require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const { conversations, createConversation } = require('@grammyjs/conversations');
const { session } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const cron = require('node-cron');
const Bottleneck = require('bottleneck');

// Инициализация бота
const bot = new Bot(process.env.BOT_API_KEY);
console.log('Bot is starting...');

// Подключаем middleware для сессий и разговоров
bot.use(session({ initial: () => ({}) }));
bot.use(conversations());

// Настройка Bottleneck
const limiter = new Bottleneck({
    maxConcurrent: 1, // Максимальное количество одновременных задач
    minTime: 1000 // Минимальное время между задачами в миллисекундах (1 секунда)
});

// Подключение к базе данных SQLite
const db = new sqlite3.Database('tasks.db', (err) => {
    if (err) {
        console.error('Could not connect to database', err);
    } else {
        console.log('Connected to SQLite database.');
    }
});

// Создание таблиц, если они не существуют
db.serialize(() => {
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
});

// Карта пользователей: Telegram ник -> ФИО и Jira логины.
const userMappings = {
    lipchinski: {
        name: "Дмитрий Селиванов",
        sxl: "d.selivanov",
        betone: "dms"
    },
    pr0spal: {
        name: "Евгений Шушков",
        sxl: "e.shushkov",
        betone: "es"
    },
    fdhsudgjdgkdfg: {
        name: "Даниил Маслов",
        sxl: "d.maslov",
        betone: "dam"
    },
    EuroKaufman: {
        name: "Даниил Баратов",
        sxl: "d.baratov",
        betone: "db"
    },
    Nikolay_Gonchar: {
        name: "Николай Гончар",
        sxl: "n.gonchar",
        betone: "ng"
    },
    KIRILlKxX: {
        name: "Кирилл Атанизяов",
        sxl: "k.ataniyazov",
        betone: "ka"
    },
    marysh353: {
        name: "Даниил Марышев",
        sxl: "d.maryshev",
        betone: "dma"
    }
};

/**
 * Функция переводит Telegram username -> ФИО.
 */
function mapTelegramUserToName(tgUsername) {
    if (!tgUsername || !userMappings[tgUsername]) return "Неизвестный пользователь";
    return userMappings[tgUsername].name;
}

/**
 * Функция получает Jira-логин пользователя в зависимости от источника.
 */
function getJiraUsername(tgUsername, source) {
    if (!tgUsername || !userMappings[tgUsername]) return null;
    if (source === 'sxl') return userMappings[tgUsername].sxl;
    if (source === 'betone') return userMappings[tgUsername].betone;
    return null;
}

/**
 * Эмоджи для приоритета.
 */
function getPriorityEmoji(priority) {
    const map = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return map[priority] || '';
}

/**
 * Возвращает текущую дату-время по Москве в формате 'yyyy-MM-dd HH:mm:ss'.
 */
function getMoscowTimestamp() {
    return DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd HH:mm:ss');
}

/**
 * Основная функция: получение задач из Jira и сохранение в базу данных.
 */
async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira('sxl', 'https://jira.sxl.team/rest/api/2/search', process.env.JIRA_PAT_SXL);
    await fetchAndStoreTasksFromJira('betone', 'https://jira.betone.team/rest/api/2/search', process.env.JIRA_PAT_BETONE);
}

/**
 * Получение и сохранение задач из конкретного Jira.
 * @param {string} source - Источник Jira ('sxl' или 'betone').
 * @param {string} url - URL API Jira.
 * @param {string} pat - Personal Access Token для Jira.
 */
async function fetchAndStoreTasksFromJira(source, url, pat) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);

        // Определение departmentQuery
        const departmentQuery = 'Техническая поддержка'; // Замените на вашу логику, если необходимо

        // Формирование JQL согласно вашей логике
        const jql = `
            project = SUPPORT AND (
                (issuetype = Infra AND status = "Open") OR
                (issuetype = Office AND status in ("Under review", "Waiting for support")) OR
                (issuetype = Prod AND status = "Waiting for Developers approval") OR
                (Отдел = "${departmentQuery}" AND status = "Open")
            )
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

        for (const issue of fetchedIssues) {
            const fields = issue.fields;

            // Определение department в зависимости от source
            let department = 'Не указан';
            if (source === 'sxl') {
                // Для SXL используем customfield_10500
                department = fields.customfield_10500?.value || 'Не указан';
            } else if (source === 'betone') {
                // Для BetOne используем customfield_10504
                department = fields.customfield_10504?.value || 'Не указан';
            }

            const resolution = fields.resolution?.name || '';
            const assigneeKey = fields.assignee?.name || '';  // Например, "d.selivanov"
            const assigneeMapped = Object.values(userMappings).find(um => um.sxl === assigneeKey || um.betone === assigneeKey);
            const assigneeName = assigneeMapped ? assigneeMapped.name : '';

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

            const existingTask = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM tasks WHERE id = ?', [taskData.id], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            if (existingTask) {
                // Обновляем существующую задачу
                db.run(
                    `UPDATE tasks
                     SET title = ?,
                         priority = ?,
                         issueType = ?,
                         department = ?,
                         resolution = ?,
                         assignee = ?,
                         source = ?,
                         archived = 0 -- Снимаем флаг архивирования
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
                // Вставляем новую задачу
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

        // Архивируем задачи, которые не были получены в текущем запросе
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
            // Если вообще нет задач, архивируем все для этого source
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
 * Функция отправки задач в Telegram канал.
 * @param {string} chatId - ID чата (канала), куда отправлять задачи.
 */
async function sendJiraTasksToChat(chatId) {
    const today = getMoscowTimestamp().split(' ')[0];
    const query = `
        SELECT *
        FROM tasks
        WHERE archived = 0
          AND department = 'Техническая поддержка'
          AND issueType IN ('Infra', 'Office', 'Prod')
          AND (lastSent IS NULL OR lastSent < date('${today}'))
          AND dateAdded >= date('now', '-30 days') -- Исключаем задачи старше 30 дней
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('sendJiraTasksToChat() error:', err);
            return;
        }

        for (const task of rows) {
            const keyboard = new InlineKeyboard();
            const jiraUrl = `https://jira.${task.source}.team/browse/${task.id}`;

            // Создаём кнопки
            keyboard
                .text('Взять в работу', `take_task:${task.id}`)
                .text('Комментарий', `comment_task:${task.id}`)
                .text('Завершить', `complete_task:${task.id}`)
                .row()
                .url('Перейти к задаче', jiraUrl);

            const messageText = `
Задача: ${task.id}
Источник: ${task.source}
Описание: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}
Исполнитель: ${task.assignee || 'не назначен'}
Департамент: ${task.department}
            `.trim();

            // Обернуть отправку сообщения в Bottleneck
            limiter.schedule(() => sendMessageWithRetry(chatId, messageText, { reply_markup: keyboard }))
                .then(() => {
                    // Обновляем поле lastSent после успешной отправки
                    db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [getMoscowTimestamp(), task.id]);
                })
                .catch((error) => {
                    console.error('Failed to send message after retries:', error);
                });
        }
    });
}

/**
 * Отправляет сообщение с обработкой ошибок 429 и повторными попытками.
 * @param {number|string} chatId - ID чата.
 * @param {string} text - Текст сообщения.
 * @param {object} options - Дополнительные опции (например, reply_markup).
 */
async function sendMessageWithRetry(chatId, text, options = {}) {
    try {
        await bot.api.sendMessage(chatId, text, options);
    } catch (error) {
        if (error.error_code === 429 && error.parameters && error.parameters.retry_after) {
            const retryAfter = error.parameters.retry_after * 1000; // Переводим в миллисекунды
            console.warn(`Rate limit exceeded. Retrying after ${retryAfter / 1000} seconds...`);

            // Ждем указанное время
            await new Promise(resolve => setTimeout(resolve, retryAfter));

            // Рекурсивно пробуем снова
            return sendMessageWithRetry(chatId, text, options);
        } else {
            // Проброс других ошибок дальше
            throw error;
        }
    }
}

/**
 * Функция для обновления assignee задачи в Jira.
 */
async function updateJiraAssignee(source, taskId, jiraUsername) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}/assignee`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        const response = await axios.put(url, { name: jiraUsername }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        console.log(`Assignee updated for ${taskId}:`, response.status);
        return true;
    } catch (error) {
        console.error(`updateJiraAssignee error for ${taskId}:`, error.response?.data || error);
        return false;
    }
}

/**
 * Функция для добавления комментария к задаче в Jira.
 */
async function updateJiraIssueComment(source, taskId, jiraUsername, commentBody) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}/comment`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        const response = await axios.post(url, { body: commentBody }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        console.log(`Comment added for ${taskId}:`, response.status);
        return true;
    } catch (error) {
        console.error(`updateJiraIssueComment error for ${taskId}:`, error.response?.data || error);
        return false;
    }
}

/**
 * Функция для перевода задачи в статус Done в Jira.
 */
async function updateJiraTaskStatus(source, taskId, transitionId) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}/transitions`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        const response = await axios.post(url, { transition: { id: transitionId } }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        console.log(`Task ${taskId} transitioned to Done:`, response.status);
        return true;
    } catch (error) {
        console.error(`updateJiraTaskStatus error for ${taskId}:`, error.response?.data || error);
        return false;
    }
}

/**
 * Conversation для добавления комментария.
 */
async function commentConversation(conversation, ctx) {
    // Получаем taskId из callbackData
    const parts = ctx.match.input.split(':'); // "comment_task:ABC-123"
    const taskId = parts[1];

    // Получаем информацию о задаче из БД
    const taskRow = await new Promise((resolve, reject) => {
        db.get('SELECT * FROM tasks WHERE id = ?', [taskId], (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });

    if (!taskRow) {
        await ctx.reply('Задача не найдена в базе данных.');
        return;
    }

    const source = taskRow.source;
    const telegramUsername = ctx.from?.username || '';
    const realName = mapTelegramUserToName(telegramUsername);
    const jiraUsername = getJiraUsername(telegramUsername, source);

    if (!jiraUsername) {
        await ctx.reply(`Не найден Jira-логин для пользователя ${telegramUsername}`);
        return;
    }

    // Запрашиваем комментарий
    await ctx.reply('Введите комментарий для задачи:');
    const { message } = await conversation.wait();

    const userComment = message.text;

    // Отправляем комментарий в Jira
    const success = await updateJiraIssueComment(source, taskId, jiraUsername, userComment);

    if (!success) {
        await ctx.reply('Ошибка при добавлении комментария в Jira.');
        return;
    }

    // Редактируем исходное сообщение, добавляя название задачи и кнопку перехода
    const callbackMsg = ctx.callbackQuery?.message;
    if (callbackMsg) {
        const jiraUrl = `https://jira.${source}.team/browse/${taskId}`;
        const keyboard = new InlineKeyboard().url('Перейти к задаче', jiraUrl);
        try {
            await bot.api.editMessageText(
                callbackMsg.chat.id,
                callbackMsg.message_id,
                `${taskRow.department}\n\nКомментарий добавлен: ${realName}\n\nНазвание задачи: ${taskRow.title}`,
                { reply_markup: keyboard }
            );
        } catch (e) {
            console.error('editMessageText (comment) error:', e);
        }
    } else {
        // Если не удалось найти сообщение для редактирования
        const jiraUrl = `https://jira.${source}.team/browse/${taskId}`;
        const keyboard = new InlineKeyboard().url('Перейти к задаче', jiraUrl);
        await ctx.reply(`${taskRow.department}\n\nКомментарий добавлен: ${realName}\n\nНазвание задачи: ${taskRow.title}`, { reply_markup: keyboard });
    }
}

/** 
 * Регистрируем conversation "commentConversation"
 */
bot.use(createConversation(commentConversation, "commentConversation"));

/**
 * Обработчики инлайн-кнопок (take_task, comment_task, complete_task).
 */
bot.callbackQuery(/^(take_task|comment_task|complete_task):(.*)$/, async (ctx) => {
    const actionType = ctx.match[1]; // take_task | comment_task | complete_task
    const taskId = ctx.match[2];
    const telegramUsername = ctx.from?.username || '';
    const realName = mapTelegramUserToName(telegramUsername);

    // Получаем задачу из БД
    const taskRow = await new Promise((resolve, reject) => {
        db.get('SELECT * FROM tasks WHERE id = ?', [taskId], (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });

    if (!taskRow) {
        await ctx.answerCallbackQuery();
        await ctx.reply('Задача не найдена в базе данных.');
        return;
    }

    const { source, department, title } = taskRow;
    const jiraUsername = getJiraUsername(telegramUsername, source);

    if (!jiraUsername) {
        await ctx.answerCallbackQuery();
        await ctx.reply(`Не найден Jira-логин для пользователя ${telegramUsername}`);
        return;
    }

    if (actionType === 'take_task') {
        // Назначаем задачу пользователю в Jira
        const success = await updateJiraAssignee(source, taskId, jiraUsername);
        await ctx.answerCallbackQuery();

        if (success) {
            // Редактируем исходное сообщение, добавляя название задачи и кнопку перехода
            const jiraUrl = `https://jira.${source}.team/browse/${taskId}`;
            const keyboard = new InlineKeyboard().url('Перейти к задаче', jiraUrl);
            try {
                await ctx.editMessageText(
                    `${department}\n\nВзял в работу: ${realName}\n\nНазвание задачи: ${title}`,
                    { reply_markup: keyboard }
                );
            } catch (e) {
                console.error('editMessageText(take_task) error:', e);
            }
        } else {
            await ctx.reply('Ошибка назначения исполнителя в Jira.');
        }
    } else if (actionType === 'comment_task') {
        // Запускаем conversation для добавления комментария
        await ctx.conversation.enter("commentConversation");
    } else if (actionType === 'complete_task') {
        // Переводим задачу в статус Done
        const transitionId = '401'; // Ваш transitionId для перевода в Done
        const success = await updateJiraTaskStatus(source, taskId, transitionId);
        await ctx.answerCallbackQuery();

        if (success) {
            // Редактируем исходное сообщение, добавляя название задачи и кнопку перехода
            const jiraUrl = `https://jira.${source}.team/browse/${taskId}`;
            const keyboard = new InlineKeyboard().url('Перейти к задаче', jiraUrl);
            try {
                await ctx.editMessageText(
                    `${department}\n\nЗавершил задачу: ${realName}\n\nНазвание задачи: ${title}`,
                    { reply_markup: keyboard }
                );
            } catch (e) {
                console.error('editMessageText(complete_task) error:', e);
            }
        } else {
            await ctx.reply('Ошибка при переводе задачи в Done в Jira.');
        }
    }
});

/**
 * Команда /report — выводит статистику по выполненным задачам за последние 30 дней.
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
 * Ежедневная очистка старых задач и связанных данных.
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

    // Чистим user_actions, если задачи уже нет
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

    // Чистим task_comments, если задачи уже нет
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
 * Проверка новых комментариев в закрытых задачах (Done) в Техподдержке каждые 5 минут.
 */
cron.schedule('*/5 * * * *', async () => {
    console.log('Checking new comments in done tasks...');
    await checkNewCommentsInDoneTasks();
});

/**
 * Функция проверки новых комментариев.
 */
async function checkNewCommentsInDoneTasks() {
    try {
        const query = `
            SELECT *
            FROM tasks
            WHERE department = 'Техническая поддержка'
              AND resolution = 'Done'
              AND archived = 0
              AND dateAdded >= date('now', '-30 days') -- Исключаем старые задачи
        `;

        db.all(query, [], async (err, tasks) => {
            if (err) {
                console.error('Error fetching done tasks for comments check:', err);
                return;
            }

            for (const task of tasks) {
                const { id, source, title } = task;

                const tableCommentInfo = await new Promise((resolve, reject) => {
                    db.get('SELECT * FROM task_comments WHERE taskId = ?', [id], (err2, row) => {
                        if (err2) reject(err2);
                        else resolve(row);
                    });
                });

                const lastSavedCommentId = tableCommentInfo ? tableCommentInfo.lastCommentId : null;

                // Запрашиваем комментарии из Jira
                const commentUrl = `https://jira.${source}.team/rest/api/2/issue/${id}/comment`;
                const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

                const response = await axios.get(commentUrl, {
                    headers: {
                        'Authorization': `Bearer ${pat}`,
                        'Accept': 'application/json'
                    }
                });

                const allComments = response.data.comments || [];
                // Сортируем по ID (предполагаем, что ID числовой)
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
Ссылка на задачу: https://jira.${source}.team/browse/${task.id}
                        `.trim();

                        // Отправляем в admin чат
                        if (process.env.ADMIN_CHAT_ID) {
                            await limiter.schedule(() => sendMessageWithRetry(process.env.ADMIN_CHAT_ID, messageText));
                        } else {
                            console.error('ADMIN_CHAT_ID is not set in .env');
                        }

                        // Обновляем lastCommentId
                        if (!newLastId || commentIdNum > parseInt(newLastId)) {
                            newLastId = comment.id;
                        }
                    }
                }

                // Обновляем или вставляем запись о последнем комментарии
                if (newLastId && newLastId !== lastSavedCommentId) {
                    if (tableCommentInfo) {
                        db.run(
                            `UPDATE task_comments
                             SET lastCommentId = ?, timestamp = ?
                             WHERE taskId = ?`,
                            [newLastId, getMoscowTimestamp(), id]
                        );
                    } else {
                        db.run(
                            `INSERT INTO task_comments (taskId, lastCommentId, timestamp)
                             VALUES (?, ?, ?)`,
                            [id, newLastId, getMoscowTimestamp()]
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
 * Обработчик channel_post для команд, отправленных в канал.
 */
bot.on('channel_post', async (ctx) => {
    const text = ctx.channelPost.text;
    const chatId = ctx.channelPost.chat.id;
    console.log(`Received channel_post from chat ID ${chatId}: ${text}`);

    if (text && text.startsWith('/start')) {
        console.log('Processing /start command in channel');
        await ctx.reply(
            'Привет! Я бот, который будет сообщать о новых задачах.\n' +
            'Используй /report для отчёта по выполненным задачам.'
        );

        await fetchAndStoreJiraTasks();
        await sendJiraTasksToChat(chatId);
    }
});

/**
 * Обработчик команды /start для личных чатов и групп.
 */
bot.command('start', async (ctx) => {
    console.log('Received /start command from:', ctx.from?.username);
    await ctx.reply(
        'Привет! Я буду сообщать о новых задачах.\n' +
        'Используй /report для отчёта по выполненным задачам.'
    );
    await fetchAndStoreJiraTasks();
    await sendJiraTasksToChat(process.env.ADMIN_CHAT_ID);

    // Обратите внимание: Планировщики cron уже определены глобально,
    // поэтому не нужно их запускать здесь снова.
});

/**
 * Глобальный обработчик ошибок.
 */
bot.catch(async (err, ctx) => {
    if (ctx && ctx.update && ctx.update.update_id) {
        console.error(`Error while handling update ${ctx.update.update_id}:`, err);
    } else {
        console.error('Error while handling update:', err);
    }

    // Проверяем, можно ли отправить сообщение об ошибке
    if (ctx && ctx.replyable) {
        try {
            await ctx.reply('Произошла ошибка при обработке вашего запроса.');
        } catch (e) {
            console.error('Error sending error message:', e);
        }
    }
});

/**
 * Утренние и ночные уведомления.
 */
cron.schedule('0 21 * * *', async () => {
    if (process.env.ADMIN_CHAT_ID) {
        await limiter.schedule(() => sendMessageWithRetry(process.env.ADMIN_CHAT_ID, 'Доброй ночи! Заполни тикет передачи смены.'));
    } else {
        console.error('ADMIN_CHAT_ID is not set in .env');
    }
});
cron.schedule('0 9 * * *', async () => {
    if (process.env.ADMIN_CHAT_ID) {
        await limiter.schedule(() => sendMessageWithRetry(process.env.ADMIN_CHAT_ID, 'Доброе утро! Проверь задачи на сегодня и начни смену.'));
    } else {
        console.error('ADMIN_CHAT_ID is not set in .env');
    }
});

/**
 * Запускаем бота.
 */
bot.start();
