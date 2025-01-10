// index.js

require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const { conversations } = require('@grammyjs/conversations');
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

// Настройка Bottleneck для управления лимитами Telegram API
const limiter = new Bottleneck({
    maxConcurrent: 1, // Максимальное количество одновременных задач
    minTime: 1000     // Минимальное время между задачами в миллисекундах (1 секунда)
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
            lastSentDate DATETIME,
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

// Карта пользователей: Telegram ник -> ФИО и Jira логины для разных источников
const userMappings = {
    lipchinski: { name: "Дмитрий Селиванов", sxl: "d.selivanov", betone: "dms" },
    pr0spal: { name: "Евгений Шушков", sxl: "e.shushkov", betone: "es" },
    fdhsudgjdgkdfg: { name: "Даниил Маслов", sxl: "d.maslov", betone: "dam" },
    EuroKaufman: { name: "Даниил Баратов", sxl: "d.baratov", betone: "db" },
    Nikolay_Gonchar: { name: "Николай Гончар", sxl: "n.gonchar", betone: "ng" },
    KIRILlKxX: { name: "Кирилл Атанизяов", sxl: "k.ataniyazov", betone: "ka" },
    marysh353: { name: "Даниил Марышев", sxl: "d.maryshev", betone: "dma" }
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
    return source === 'sxl' ? userMappings[tgUsername].sxl :
           source === 'betone' ? userMappings[tgUsername].betone : null;
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

// Список issuetype, для которых нельзя брать в работу и завершать задачи
const nonEditableIssueTypes = ['Infra', 'Office', 'Prod'];

/**
 * Основная функция: получение задач из Jira и сохранение в базу данных.
 * @param {string} source - Источник Jira ('sxl' или 'betone').
 * @param {string} url - URL API Jira.
 * @param {string} pat - Personal Access Token для Jira.
 * @param {string} jql - JQL запрос для выборки задач.
 */
async function fetchAndStoreJiraTasksFromSource(source, url, pat, jql) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);

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
                department = fields.customfield_10500?.value || 'Не указан';
            } else if (source === 'betone') {
                department = fields.customfield_10504?.value || 'Не указан';
            }

            const resolution = fields.resolution?.name || '';
            const assigneeKey = fields.assignee?.name || '';
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
                     SET title = ?, priority = ?, issueType = ?, department = ?, resolution = ?, assignee = ?, source = ?, archived = 0, archivedDate = NULL
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
                    ],
                    function(err) {
                        if (err) {
                            console.error(`Error updating task ${taskData.id}:`, err);
                        } else {
                            console.log(`Task ${taskData.id} updated.`);
                        }
                    }
                );
            } else {
                // Вставляем новую задачу
                db.run(
                    `INSERT INTO tasks
                     (id, title, priority, issueType, department, resolution, assignee, dateAdded, lastSentDate, source, archived, archivedDate)
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
                    ],
                    function(err) {
                        if (err) {
                            console.error(`Error inserting task ${taskData.id}:`, err);
                        } else {
                            console.log(`Task ${taskData.id} inserted.`);
                        }
                    }
                );
            }

            // Fetch the latest comment
            const commentUrl = `https://jira.${source}.team/rest/api/2/issue/${taskData.id}/comment`;
            const commentResponse = await axios.get(commentUrl, {
                headers: {
                    'Authorization': `Bearer ${pat}`,
                    'Accept': 'application/json'
                }
            });

            const allComments = commentResponse.data.comments || [];
            if (allComments.length > 0) {
                // Assuming comments are sorted by creation date
                const latestComment = allComments[allComments.length - 1];
                const latestCommentId = latestComment.id;

                // Update or insert into task_comments
                const existingCommentInfo = await new Promise((resolve, reject) => {
                    db.get('SELECT * FROM task_comments WHERE taskId = ?', [taskData.id], (err, row) => {
                        if (err) reject(err);
                        else resolve(row);
                    });
                });

                if (existingCommentInfo) {
                    db.run(
                        `UPDATE task_comments
                         SET lastCommentId = ?, timestamp = ?
                         WHERE taskId = ?`,
                        [latestCommentId, getMoscowTimestamp(), taskData.id],
                        function(err) {
                            if (err) {
                                console.error(`Error updating comments for task ${taskData.id}:`, err);
                            } else {
                                console.log(`Comments for task ${taskData.id} updated.`);
                            }
                        }
                    );
                } else {
                    db.run(
                        `INSERT INTO task_comments (taskId, lastCommentId, timestamp)
                         VALUES (?, ?, ?)`,
                        [taskData.id, latestCommentId, getMoscowTimestamp()],
                        function(err) {
                            if (err) {
                                console.error(`Error inserting comments for task ${taskData.id}:`, err);
                            } else {
                                console.log(`Comments for task ${taskData.id} inserted.`);
                            }
                        }
                    );
                }
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
    }

/**
 * Функция отправки задач в Telegram канал с заданным JQL фильтром.
 * @param {string} chatId - ID чата (канала), куда отправлять задачи.
 * @param {string} jql - JQL запрос для выборки задач.
 * @param {string} scheduleDescription - Описание расписания для логирования.
 */
async function sendFilteredJiraTasksToChat(chatId, jql, scheduleDescription) {
    const today = DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd');

    const query = `
        SELECT *
        FROM tasks
        WHERE archived = 0
          AND (${jql})
          AND (lastSentDate IS NULL OR lastSentDate < date('${today}'))
          AND date(dateAdded) >= date('now', '-30 days') -- Исключаем задачи старше 30 дней
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error(`sendFilteredJiraTasksToChat() error (${scheduleDescription}):`, err);
            return;
        }

        for (const task of rows) {
            const keyboard = new InlineKeyboard();
            const jiraUrl = `https://jira.${task.source}.team/browse/${task.id}`;

            // Добавляем кнопки "Взять в работу" и "Завершить", если issuetype разрешен
            if (!nonEditableIssueTypes.includes(task.issueType)) {
                keyboard
                    .text('Взять в работу', `take_task:${task.id}`)
                    .text('Завершить', `complete_task:${task.id}`)
                    .row();
            }

            // Всегда добавляем кнопку "Перейти к задаче"
            keyboard.url('Перейти к задаче', jiraUrl);

            const messageText = `
${task.department} - ${task.id}

Взял в работу: ${task.assignee || 'Не назначен'}
Название задачи: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}
            `.trim();

            // Отправляем сообщение через Bottleneck
            limiter.schedule(() => sendMessageWithRetry(chatId, messageText, { reply_markup: keyboard }))
                .then(() => {
                    // Обновляем поле lastSentDate после успешной отправки
                    db.run('UPDATE tasks SET lastSentDate = ? WHERE id = ?', [getMoscowTimestamp(), task.id], function(err) {
                        if (err) {
                            console.error(`Error updating lastSentDate for task ${task.id}:`, err);
                        } else {
                            console.log(`lastSentDate updated for task ${task.id}.`);
                        }
                    });
                })
                .catch((error) => {
                    console.error(`Failed to send message after retries (${task.id}):`, error);
                });
        }
    });
}

/**
 * Функция отправки задач "Техническая поддержка" со статусом "Open" раз в сутки.
 */
async function sendDailyTechnicalSupportTasks() {
    const dailyJql = `
        department = "Техническая поддержка" AND status = "Open"
    `.replace(/\n/g, ' ').trim();

    await sendFilteredJiraTasksToChat(process.env.ADMIN_CHAT_ID, dailyJql, 'Daily Technical Support Tasks');
}

/**
 * Функция отправки задач с определёнными фильтрами раз в три дня.
 */
async function sendEveryThreeDaysSpecialTasks() {
    const specialJql = `
        (issuetype = Infra AND status = "Open") OR
        (issuetype = Office AND status = "Under review") OR
        (issuetype = Office AND status = "Waiting for support") OR
        (issuetype = Prod AND status = "Waiting for Developers approval")
    `.replace(/\n/g, ' ').trim();

    await sendFilteredJiraTasksToChat(process.env.ADMIN_CHAT_ID, specialJql, 'Every Three Days Special Tasks');
}

/**
 * Функция отправки задач в Telegram канал.
 * @param {string} chatId - ID чата (канала), куда отправлять задачи.
 */
async function sendJiraTasksToChat(chatId) {
    const today = DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd');

    const generalJql = `
        project = SUPPORT AND (
            (issuetype = Infra AND status = "Open") OR
            (issuetype = Office AND status in ("Under review", "Waiting for support")) OR
            (issuetype = Prod AND status = "Waiting for Developers approval") OR
            (department = "Техническая поддержка" AND status = "Open")
        )
    `.replace(/\n/g, ' ').trim();

    const query = `
        SELECT *
        FROM tasks
        WHERE archived = 0
          AND (${generalJql})
          AND (lastSentDate IS NULL OR lastSentDate < date('${today}'))
          AND date(dateAdded) >= date('now', '-30 days') -- Исключаем задачи старше 30 дней
          AND date(dateAdded) = date('${today}') -- Отправляем только сегодняшние задачи
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('sendJiraTasksToChat() error:', err);
            return;
        }

        for (const task of rows) {
            const keyboard = new InlineKeyboard();
            const jiraUrl = `https://jira.${task.source}.team/browse/${task.id}`;

            // Добавляем кнопки "Взять в работу" и "Завершить", если issuetype разрешен
            if (!nonEditableIssueTypes.includes(task.issueType)) {
                keyboard
                    .text('Взять в работу', `take_task:${task.id}`)
                    .text('Завершить', `complete_task:${task.id}`)
                    .row();
            }

            // Всегда добавляем кнопку "Перейти к задаче"
            keyboard.url('Перейти к задаче', jiraUrl);

            const messageText = `
${task.department} - ${task.id}

Взял в работу: ${task.assignee || 'Не назначен'}
Название задачи: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}
            `.trim();

            // Отправляем сообщение через Bottleneck
            limiter.schedule(() => sendMessageWithRetry(chatId, messageText, { reply_markup: keyboard }))
                .then(() => {
                    // Обновляем поле lastSentDate после успешной отправки
                    db.run('UPDATE tasks SET lastSentDate = ? WHERE id = ?', [getMoscowTimestamp(), task.id], function(err) {
                        if (err) {
                            console.error(`Error updating lastSentDate for task ${task.id}:`, err);
                        } else {
                            console.log(`lastSentDate updated for task ${task.id}.`);
                        }
                    });
                })
                .catch((error) => {
                    console.error('Failed to send message after retries:', error);
                });
        }
    });
}

/**
 * Функция отправки сообщений с обработкой ошибок 429 и повторными попытками.
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
 * Обработчики инлайн-кнопок (take_task, complete_task).
 */
bot.callbackQuery(/^(take_task|complete_task):(.*)$/, async (ctx) => {
    const [ , actionType, taskId ] = ctx.match;
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

    const { source, department, title, issueType } = taskRow;
    const jiraUsername = getJiraUsername(telegramUsername, source);

    if (!jiraUsername) {
        await ctx.answerCallbackQuery();
        await ctx.reply(`Не найден Jira-логин для пользователя ${telegramUsername}`);
        return;
    }

    // Проверяем, разрешено ли редактировать задачу по её issuetype
    if (nonEditableIssueTypes.includes(issueType)) {
        await ctx.answerCallbackQuery();
        await ctx.reply('Действия "Взять в работу" и "Завершить" недоступны для этой задачи.');
        return;
    }

    if (actionType === 'take_task') {
        try {
            // Назначаем задачу пользователю в Jira
            const assignSuccess = await updateJiraAssignee(source, taskId, jiraUsername);
            if (!assignSuccess) {
                await ctx.reply('Ошибка назначения исполнителя в Jira.');
                await ctx.answerCallbackQuery();
                return;
            }

            // Определяем transitionId для перевода в статус "In Progress"
            let transitionId;
            if (source === 'sxl') {
                transitionId = '221'; // Ваш transitionId для sxl (например, "In Progress")
            } else if (source === 'betone') {
                transitionId = '201'; // Ваш transitionId для betone
            } else {
                console.error('Invalid source specified');
                await ctx.reply('Неверный источник задачи.');
                await ctx.answerCallbackQuery();
                return;
            }

            // Переводим задачу в статус "In Progress" или аналогичный
            const transitionSuccess = await updateJiraTaskStatus(source, taskId, transitionId);
            if (!transitionSuccess) {
                await ctx.reply('Ошибка перевода задачи в статус "В процессе" в Jira.');
                await ctx.answerCallbackQuery();
                return;
            }

            // Обновляем поле lastSentDate
            db.run('UPDATE tasks SET lastSentDate = ? WHERE id = ?', [getMoscowTimestamp(), taskId], function(err) {
                if (err) {
                    console.error(`Error updating lastSentDate for task ${taskId}:`, err);
                } else {
                    console.log(`lastSentDate updated for task ${taskId}.`);
                }
            });

            // Редактируем исходное сообщение, добавляя название задачи и кнопку перехода
            const jiraUrl = `https://jira.${source}.team/browse/${taskId}`;
            const keyboard = new InlineKeyboard().url('Перейти к задаче', jiraUrl);
            const updatedMessage = `
${department} - ${taskId}

Взял в работу: ${realName}
Название задачи: ${title}
Приоритет: ${getPriorityEmoji(taskRow.priority)}
Тип задачи: ${task.issueType}
            `.trim();

            await ctx.editMessageText(updatedMessage, { reply_markup: keyboard });
            await ctx.answerCallbackQuery();

        } catch (error) {
            console.error('Error during take_task action:', error);
            await ctx.reply('Произошла ошибка при взятии задачи в работу.');
            await ctx.answerCallbackQuery();
        }

    } else if (actionType === 'complete_task') {
        try {
            // Переводим задачу в статус Done
            const transitionId = '401'; // Ваш transitionId для перевода в Done
            const transitionSuccess = await updateJiraTaskStatus(source, taskId, transitionId);
            if (!transitionSuccess) {
                await ctx.reply('Ошибка перевода задачи в статус Done в Jira.');
                await ctx.answerCallbackQuery();
                return;
            }

            // Обновляем поле resolution
            const resolutionSuccess = await updateJiraTaskResolution(source, taskId, 'Done');
            if (!resolutionSuccess) {
                await ctx.reply('Ошибка при обновлении resolution в Jira.');
                await ctx.answerCallbackQuery();
                return;
            }

            // Обновляем поле lastSentDate
            db.run('UPDATE tasks SET lastSentDate = ? WHERE id = ?', [getMoscowTimestamp(), taskId], function(err) {
                if (err) {
                    console.error(`Error updating lastSentDate for task ${taskId}:`, err);
                } else {
                    console.log(`lastSentDate updated for task ${taskId}.`);
                }
            });

            // Редактируем исходное сообщение, добавляя название задачи и кнопку перехода
            const jiraUrl = `https://jira.${source}.team/browse/${taskId}`;
            const keyboard = new InlineKeyboard().url('Перейти к задаче', jiraUrl);
            const updatedMessage = `
${department} - ${taskId}

Завершил задачу: ${realName}
Название задачи: ${title}
Приоритет: ${getPriorityEmoji(taskRow.priority)}
Тип задачи: ${task.issueType}
            `.trim();

            await ctx.editMessageText(updatedMessage, { reply_markup: keyboard });
            await ctx.answerCallbackQuery();

        } catch (error) {
            console.error('Error during complete_task action:', error);
            await ctx.reply('Произошла ошибка при завершении задачи.');
            await ctx.answerCallbackQuery();
        }
    }
});

/**
 * Команда /start — инициализация бота.
 * При запуске бота собирает задачи за последние 30 дней, но отправляет только сегодняшние.
 */
bot.command('start', async (ctx) => {
    console.log('Received /start command from:', ctx.from?.username);
    await ctx.reply(
        'Привет! Я буду сообщать о новых задачах.\n' +
        'Используй /report для отчёта по выполненным задачам.'
    );
    await fetchAndStoreJiraTasks();
    await sendDailyTechnicalSupportTasks(); // Отправка ежедневных задач при запуске
    await sendJiraTasksToChat(process.env.ADMIN_CHAT_ID); // Отправка общих задач при запуске
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
                stats[name] = (stats[name] || 0) + 1;
            }

            let reportMessage = 'Отчёт по выполненным задачам (Техподдержка) за последние 30 дней:\n\n';
            for (const [name, count] of Object.entries(stats)) {
                reportMessage += `${name}: ${count} задач(и)\n`;
            }

            await ctx.reply(reportMessage);
        });
    } catch (error) {
        console.error('Error in /report command:', error);
        await ctx.reply('Произошла ошибка при формировании отчёта.');
    }
});

/**
 * Функция для архивирования задачи.
 * Помечает задачу как архивированную и устанавливает дату архивирования.
 * @param {string} taskId - ID задачи.
 */
function archiveTask(taskId) {
    db.run(
        `UPDATE tasks
         SET archived = 1,
             archivedDate = ?
         WHERE id = ?`,
        [getMoscowTimestamp(), taskId],
        function(err) {
            if (err) {
                console.error(`Error archiving task ${taskId}:`, err);
            } else {
                console.log(`Task ${taskId} archived.`);
            }
        }
    );
}

/**
 * Функция для обновления поля resolution задачи в Jira.
 * @param {string} source - Источник Jira ('sxl' или 'betone').
 * @param {string} taskId - ID задачи.
 * @param {string} resolutionValue - Значение для поля resolution (например, 'Done').
 * @returns {boolean} - Возвращает true при успешном обновлении, иначе false.
 */
async function updateJiraTaskResolution(source, taskId, resolutionValue) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        const response = await axios.put(url, {
            fields: {
                resolution: { name: resolutionValue }
            }
        }, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        console.log(`Resolution updated for ${taskId}:`, response.status);
        return true;
    } catch (error) {
        console.error(`updateJiraTaskResolution error for ${taskId}:`, error.response?.data || error);
        return false;
    }
}

/**
 * Функция для перевода задачи в статус Done в Jira.
 * @param {string} source - Источник Jira ('sxl' или 'betone').
 * @param {string} taskId - ID задачи.
 * @param {string} transitionId - ID перехода для статуса Done.
 * @returns {boolean} - Возвращает true при успешном обновлении, иначе false.
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

        console.log(`Task ${taskId} transitioned:`, response.status);
        return true;
    } catch (error) {
        console.error(`updateJiraTaskStatus error for ${taskId}:`, error.response?.data || error);
        return false;
    }
}

/**
 * Функция для обновления assignee задачи в Jira.
 * @param {string} source - Источник Jira ('sxl' или 'betone').
 * @param {string} taskId - ID задачи.
 * @param {string} jiraUsername - Jira-логин пользователя.
 * @returns {boolean} - Возвращает true при успешном обновлении, иначе false.
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
 * Функция проверки новых комментариев в завершенных задачах (Done) для отдела "Техническая поддержка".
 */
async function checkNewCommentsInDoneTechnicalSupportTasks() {
    try {
        const thirtyDaysAgo = DateTime.now().setZone('Europe/Moscow').minus({ days: 30 }).toISODate();

        const query = `
            SELECT *
            FROM tasks
            WHERE resolution = 'Done'
              AND department = 'Техническая поддержка'
              AND date(dateAdded) >= date(?)
              AND archived = 0
        `;

        const doneTasks = await new Promise((resolve, reject) => {
            db.all(query, [thirtyDaysAgo], (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        for (const task of doneTasks) {
            const { id, source, title, priority, issueType } = task;

            const commentInfo = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM task_comments WHERE taskId = ?', [id], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            const lastSavedCommentId = commentInfo ? commentInfo.lastCommentId : null;

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
            // Сортируем комментарии по дате создания
            allComments.sort((a, b) => new Date(a.created) - new Date(b.created));

            let latestNewComment = null;
            let newLastCommentId = lastSavedCommentId;

            for (const comment of allComments) {
                const commentId = comment.id;
                if (!lastSavedCommentId || commentId > lastSavedCommentId) {
                    latestNewComment = comment;
                    if (!newLastCommentId || commentId > newLastCommentId) {
                        newLastCommentId = commentId;
                    }
                }
            }

            if (latestNewComment) {
                const { author, body } = latestNewComment;
                const authorName = author?.displayName || 'Неизвестный автор';
                const messageText = `
📝 *Новый комментарий к завершенной задаче*

*Задача:* ${task.id}
*Источник:* ${source}
*Описание:* ${title}
*Приоритет:* ${getPriorityEmoji(priority)}
*Тип задачи:* ${issueType}

*Автор комментария:* ${authorName}
*Комментарий:* ${body}
*Ссылка на задачу:* [${task.id}](https://jira.${source}.team/browse/${task.id})
                `.trim();

                // Отправляем сообщение в admin чат
                if (process.env.ADMIN_CHAT_ID) {
                    await limiter.schedule(() => sendMessageWithRetry(process.env.ADMIN_CHAT_ID, messageText, { parse_mode: 'Markdown' }));
                } else {
                    console.error('ADMIN_CHAT_ID is not set in .env');
                }

                // Обновляем или вставляем запись о последнем комментарии
                if (commentInfo) {
                    db.run(
                        `UPDATE task_comments
                         SET lastCommentId = ?, timestamp = ?
                         WHERE taskId = ?`,
                        [newLastCommentId, getMoscowTimestamp(), id],
                        function(err) {
                            if (err) {
                                console.error(`Error updating comments for task ${id}:`, err);
                            } else {
                                console.log(`Comments for task ${id} updated.`);
                            }
                        }
                    );
                } else {
                    db.run(
                        `INSERT INTO task_comments (taskId, lastCommentId, timestamp)
                         VALUES (?, ?, ?)`,
                        [id, newLastCommentId, getMoscowTimestamp()],
                        function(err) {
                            if (err) {
                                console.error(`Error inserting comments for task ${id}:`, err);
                            } else {
                                console.log(`Comments for task ${id} inserted.`);
                            }
                        }
                    );
                }
            }
        }
    }

/**
 * Функция проверки новых комментариев в архивированных задачах.
 */
async function checkNewCommentsInArchivedTasks() {
    try {
        const query = `
            SELECT *
            FROM tasks
            WHERE archived = 1
        `;
        const archivedTasks = await new Promise((resolve, reject) => {
            db.all(query, [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        for (const task of archivedTasks) {
            const { id, source, title, priority, issueType, resolution } = task;

            const commentInfo = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM task_comments WHERE taskId = ?', [id], (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                });
            });

            const lastSavedCommentId = commentInfo ? commentInfo.lastCommentId : null;

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
            // Сортируем комментарии по дате создания
            allComments.sort((a, b) => new Date(a.created) - new Date(b.created));

            let latestNewComment = null;
            let newLastCommentId = lastSavedCommentId;

            for (const comment of allComments) {
                const commentId = comment.id;
                if (!lastSavedCommentId || commentId > lastSavedCommentId) {
                    latestNewComment = comment;
                    if (!newLastCommentId || commentId > newLastCommentId) {
                        newLastCommentId = commentId;
                    }
                }
            }

            if (latestNewComment && resolution === 'Done') {
                const { author, body } = latestNewComment;
                const authorName = author?.displayName || 'Неизвестный автор';
                const messageText = `
📝 *Новый комментарий к архивированной задаче*

*Задача:* ${task.id}
*Источник:* ${source}
*Описание:* ${title}
*Приоритет:* ${getPriorityEmoji(priority)}
*Тип задачи:* ${issueType}

*Автор комментария:* ${authorName}
*Комментарий:* ${body}
*Ссылка на задачу:* [${task.id}](https://jira.${source}.team/browse/${task.id})
                `.trim();

                // Отправляем сообщение в admin чат
                if (process.env.ADMIN_CHAT_ID) {
                    await limiter.schedule(() => sendMessageWithRetry(process.env.ADMIN_CHAT_ID, messageText, { parse_mode: 'Markdown' }));
                } else {
                    console.error('ADMIN_CHAT_ID is not set in .env');
                }

                // Обновляем или вставляем запись о последнем комментарии
                if (commentInfo) {
                    db.run(
                        `UPDATE task_comments
                         SET lastCommentId = ?, timestamp = ?
                         WHERE taskId = ?`,
                        [newLastCommentId, getMoscowTimestamp(), id],
                        function(err) {
                            if (err) {
                                console.error(`Error updating comments for task ${id}:`, err);
                            } else {
                                console.log(`Comments for task ${id} updated.`);
                            }
                        }
                    );
                } else {
                    db.run(
                        `INSERT INTO task_comments (taskId, lastCommentId, timestamp)
                         VALUES (?, ?, ?)`,
                        [id, newLastCommentId, getMoscowTimestamp()],
                        function(err) {
                            if (err) {
                                console.error(`Error inserting comments for task ${id}:`, err);
                            } else {
                                console.log(`Comments for task ${id} inserted.`);
                            }
                        }
                    );
                }
            }
        }
    }

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
 * Cron-задача для периодического выполнения fetchAndStoreJiraTasksFromSource каждую минуту.
 */
cron.schedule('* * * * *', async () => {
    console.log('Running cron job: fetchAndStoreJiraTasksFromSource every minute');

    // JQL для общих задач (может быть адаптирован под ваши нужды)
    const generalJql = `
        project = SUPPORT AND (
            (issuetype = Infra AND status = "Open") OR
            (issuetype = Office AND status in ("Under review", "Waiting for support")) OR
            (issuetype = Prod AND status = "Waiting for Developers approval") OR
            (department = "Техническая поддержка" AND status = "Open")
        )
    `.replace(/\n/g, ' ').trim();

    // Запуск для источника 'sxl'
    await fetchAndStoreJiraTasksFromSource(
        'sxl',
        'https://jira.sxl.team/rest/api/2/search',
        process.env.JIRA_PAT_SXL,
        generalJql
    );

    // Пример для другого источника 'betone' (если необходимо)
    /*
    const betoneJql = `
        project = BETONE AND (
            ... ваш JQL для betone ...
        )
    `.replace(/\n/g, ' ').trim();

    await fetchAndStoreJiraTasksFromSource(
        'betone',
        'https://jira.betone.team/rest/api/2/search',
        process.env.JIRA_PAT_BETONE,
        betoneJql
    );
    */
});

/**
 * Cron-задача для отправки задач "Техническая поддержка" раз в сутки.
 */
cron.schedule('0 10 * * *', async () => { // Например, в 10:00 утра
    console.log('Running cron job: sendDailyTechnicalSupportTasks');
    await sendDailyTechnicalSupportTasks();
});

/**
 * Cron-задача для отправки специальных задач каждые три дня.
 */
cron.schedule('0 12 */3 * *', async () => { // Например, в 12:00 дня каждые три дня
    console.log('Running cron job: sendEveryThreeDaysSpecialTasks');
    await sendEveryThreeDaysSpecialTasks();
});

/**
 * Cron-задача для проверки и отправки новых комментариев каждые 5 минут.
 */
cron.schedule('*/5 * * * *', async () => {
    console.log('Running cron job: checkNewCommentsInDoneTechnicalSupportTasks and checkNewCommentsInArchivedTasks every 5 minutes');
    await checkNewCommentsInDoneTechnicalSupportTasks();
    await checkNewCommentsInArchivedTasks();
});

/**
 * Ежедневная очистка старых архивированных задач и связанных данных.
 */
cron.schedule('0 0 * * *', () => { // В полночь
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
 * Запуск бота.
 */
bot.start();
