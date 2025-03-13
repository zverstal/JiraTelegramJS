require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const cron = require('node-cron');
const Bottleneck = require('bottleneck');

// Подключение к Confluence
const Confluence = require('confluence-api');  // без фигурных скобок

// Создаем экземпляр бота
const bot = new Bot(process.env.BOT_API_KEY);
// Создаем базу данных
const db = new sqlite3.Database('tasks.db');

// Confluence API настройки (убедитесь, что у вас в .env правильные переменные)
const confluence = new Confluence({
    username: process.env.CONFLUENCE_USERNAME,
    password: process.env.CONFLUENCE_API_TOKEN,
    baseUrl: 'https://wiki.sxl.team'
});

// Функция для получения текущего времени Москвы в формате 'yyyy-MM-dd HH:mm:ss'
function getMoscowTimestamp() {
    const moscowTime = DateTime.now().setZone('Europe/Moscow');
    return moscowTime.toFormat('yyyy-MM-dd HH:mm:ss');
}

// Инициализация таблиц в базе (если не существуют)
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

    // Таблица для хранения последних комментариев задач
    db.run(`CREATE TABLE IF NOT EXISTS task_comments (
        taskId TEXT PRIMARY KEY,
        lastCommentId TEXT,
        assignee TEXT,
        FOREIGN KEY(taskId) REFERENCES tasks(id)
    )`);
});

// Маппинг приоритетов задач в эмодзи
function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

// Генерация URL для Jira задачи
function getTaskUrl(source, taskId) {
    return `https://jira.${source}.team/browse/${taskId}`;
}

// Сопоставление Telegram username → ФИО
const usernameMappings = {
    "lipchinski": "Дмитрий Селиванов",
    "pr0spal": "Евгений Шушков",
    "fdhsudgjdgkdfg": "Даниил Маслов",
    "EuroKaufman": "Даниил Баратов",
    "Nikolay_Gonchar": "Николай Гончар",
    "KIRILlKxX": "Кирилл Атанизяов",
    "marysh353": "Даниил Марышев"
};

// Сопоставление Telegram username → Jira username (по разным источникам)
const jiraUserMappings = {
    "lipchinski": { "sxl": "d.selivanov", "betone": "dms" },
    "pr0spal": { "sxl": "e.shushkov", "betone": "es" },
    "fdhsudgjdgkdfg": { "sxl": "d.maslov", "betone": "dam" },
    "EuroKaufman": { "sxl": "d.baratov", "betone": "db" },
    "Nikolay_Gonchar": { "sxl": "n.gonchar", "betone": "ng" },
    "KIRILlKxX": { "sxl": "k.ataniyazov", "betone": "ka" },
    "marysh353": { "sxl": "d.maryshev", "betone": "dma" }
};

//---------------------------------------------------------------------
// Функции для работы с Jira
//---------------------------------------------------------------------

// Запрашиваем задачи сразу из 2-х JIRA (sxl и betone)
async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira('sxl', 'https://jira.sxl.team/rest/api/2/search', process.env.JIRA_PAT_SXL, 'Техническая поддержка');
    await fetchAndStoreTasksFromJira('betone', 'https://jira.betone.team/rest/api/2/search', process.env.JIRA_PAT_BETONE, 'Техническая поддержка');
}

// Универсальная функция для запросов к JIRA и сохранения в локальную базу
async function fetchAndStoreTasksFromJira(source, url, pat, ...departments) {
    try {
        console.log(`Fetching tasks from ${source} Jira...`);
        // Формируем JQL
        const departmentQuery = departments.map(dep => `"${dep}"`).join(" OR Отдел = ");
        let jql;

        if (source === 'sxl') {
            // JQL запрос для задач DevOps и Support
            jql = `\n                project = SUPPORT AND (\n                    (issuetype = Infra AND status = "Open") OR\n                    (issuetype = Office AND status = "Under review") OR\n                    (issuetype = Office AND status = "Waiting for support") OR\n                    (issuetype = Prod AND status = "Waiting for Developers approval") OR\n                    (Отдел = ${departmentQuery} AND status = "Open")\n                )\n            `;
        } else {
            // Запрос для Технической поддержки (betone)
            jql = `project = SUPPORT AND (Отдел = ${departmentQuery}) AND status = "Open"`;
        }

        // Делаем запрос к Jira
        const response = await axios.get(url, {
            headers: {
                'Authorization': `Bearer ${pat}`,
                'Accept': 'application/json'
            },
            params: { jql }
        });
        console.log(`${source} Jira API response:`, response.data);

        const fetchedTaskIds = response.data.issues.map(issue => issue.key);

        // Удаляем из локальной БД задачи, которых больше нет в JIRA
        await new Promise((resolve, reject) => {
            const placeholders = fetchedTaskIds.map(() => '?').join(',');
            db.run(
                `DELETE FROM tasks WHERE id NOT IN (${placeholders}) AND source = ?`,
                [...fetchedTaskIds, source],
                function(err) {
                    if (err) {
                        reject(err);
                        console.error(`Error deleting tasks from ${source} Jira:`, err);
                    } else {
                        resolve();
                    }
                }
            );
        });

        // Обновляем или вставляем задачи из Jira
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
                source: source
            };

            // Проверяем, есть ли в локальной БД
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
                // Обновим
                db.run(
                    'UPDATE tasks SET title = ?, priority = ?, issueType = ?, department = ?, source = ? WHERE id = ?',
                    [task.title, task.priority, task.issueType, task.department, task.source, task.id]
                );
            } else {
                // Вставим новую
                db.run(
                    'INSERT INTO tasks (id, title, priority, issueType, department, dateAdded, lastSent, source) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)',
                    [task.id, task.title, task.priority, task.issueType, task.department, task.dateAdded, task.source]
                );
            }
        }
    } catch (error) {
        console.error(`Error fetching and storing tasks from ${source} Jira:`, error);
    }
}

//---------------------------------------------------------------------
// Отправка задач в Telegram
//---------------------------------------------------------------------
async function sendJiraTasks(ctx) {
    // Вытаскиваем дату ("2025-03-10" например)
    const today = getMoscowTimestamp().split(' ')[0];
    // Запрос для задач
    const query = `\n        SELECT * FROM tasks WHERE \n        (department = "Техническая поддержка" AND (lastSent IS NULL OR lastSent < date('${today}')))\n        OR\n        (issueType IN ('Infra', 'Office', 'Prod') AND (lastSent IS NULL OR lastSent < datetime('now', '-3 days')))\n        ORDER BY CASE \n            WHEN department = 'Техническая поддержка' THEN 1 \n            ELSE 2 \n        END\n    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('Error fetching tasks:', err);
            return;
        }

        // Для каждой задачи формируем сообщение
        for (const task of rows) {
            const keyboard = new InlineKeyboard();

            if (task.department === "Техническая поддержка") {
                keyboard.text('Взять в работу', `take_task:${task.id}`);
                keyboard.url('Перейти к задаче', getTaskUrl(task.source, task.id));
            } else if (['Infra', 'Office', 'Prod'].includes(task.issueType)) {
                keyboard.url('Перейти к задаче', getTaskUrl(task.source, task.id));
            }

            const messageText = `Задача: ${task.id}\n` +
                `Источник: ${task.source}\n` +
                `Ссылка: ${getTaskUrl(task.source, task.id)}\n` +
                `Описание: ${task.title}\n` +
                `Приоритет: ${getPriorityEmoji(task.priority)}\n` +
                `Тип задачи: ${task.issueType}`;

            console.log('Sending message to Telegram:', messageText);

            await ctx.reply(messageText, { reply_markup: keyboard });

            // Обновляем lastSent
            const moscowTimestamp = getMoscowTimestamp();
            console.log(`Updating lastSent for task ${task.id} to: ${moscowTimestamp}`);
            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [moscowTimestamp, task.id]);
        }
    });
}

//---------------------------------------------------------------------
// Проверка новых комментариев
//---------------------------------------------------------------------
async function checkForNewComments() {
    try {
        // JQL
        const jql = `project = SUPPORT AND Отдел = "Техническая поддержка" AND status in (Done, Awaiting, "Awaiting implementation") AND updated >= -2d`;
        const sources = ['sxl', 'betone'];

        // Jira usernames, которых игнорируем
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
                    params: {
                        jql,
                        maxResults: 50,
                        startAt,
                        fields: 'comment,assignee,summary,priority,issuetype'
                    }
                });

                const issues = response.data.issues;
                total = response.data.total;

                for (const issue of issues) {
                    const taskId = issue.key;
                    const comments = issue.fields.comment.comments;
                    if (comments.length === 0) continue;

                    const lastComment = comments[comments.length - 1];
                    const lastCommentId = lastComment.id;
                    const author = lastComment.author?.name || 'Не указан';

                    db.get('SELECT lastCommentId FROM task_comments WHERE taskId = ?', [taskId], (err, row) => {
                        if (err) {
                            console.error('Error fetching last comment from DB:', err);
                            return;
                        }

                        // Если нет записи в таблице task_comments
                        if (!row) {
                            // При первом комментарии, если автор не в excluded
                            if (!excludedAuthors.includes(author)) {
                                sendTelegramMessage(taskId, source, issue, lastComment, author);
                            }

                            db.run(
                                'INSERT INTO task_comments (taskId, lastCommentId, assignee) VALUES (?, ?, ?)',
                                [taskId, lastCommentId, issue.fields.assignee?.displayName || 'Не указан']
                            );
                        } else if (row.lastCommentId !== lastCommentId) {
                            // Если комментарий действительно новый
                            if (!excludedAuthors.includes(author)) {
                                sendTelegramMessage(taskId, source, issue, lastComment, author);
                            }

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

//---------------------------------------------------------------------
// Лимитер для отправки сообщений (чтобы не было спама)
//---------------------------------------------------------------------
const limiter = new Bottleneck({
    minTime: 2000,     // Минимум 2 секунды между запросами
    maxConcurrent: 1   // Один запрос одновременно
});

const sendMessageWithLimiter = limiter.wrap(async (chatId, messageText, options) => {
    try {
        console.log(`Sending message to Telegram: ${messageText}`);
        await bot.api.sendMessage(chatId, messageText, options);
    } catch (error) {
        console.error('Error in sendMessageWithLimiter:', error);
        throw error;
    }
});

//---------------------------------------------------------------------
// Функция отправки уведомления о новом комментарии
//---------------------------------------------------------------------
function sendTelegramMessage(taskId, source, issue, lastComment, author) {
    const keyboard = new InlineKeyboard();
    keyboard.url('Перейти к задаче', getTaskUrl(source, taskId));

    const messageText = `В задаче появился новый комментарий:\n\n` +
        `Задача: ${taskId}\n` +
        `Источник: ${source}\n` +
        `Ссылка: ${getTaskUrl(source, taskId)}\n` +
        `Описание: ${issue.fields.summary}\n` +
        `Приоритет: ${getPriorityEmoji(issue.fields.priority?.name || 'Не указан')}\n` +
        `Тип задачи: ${issue.fields.issuetype?.name || 'Не указан'}\n` +
        `Исполнитель: ${issue.fields.assignee?.displayName || 'Не указан'}\n` +
        `Автор комментария: ${author}\n` +
        `Комментарий: ${lastComment.body}`;

    sendMessageWithLimiter(process.env.ADMIN_CHAT_ID, messageText, {
        reply_markup: keyboard
    }).catch(err => {
        console.error('Error sending message to Telegram:', err);
    });
}

//---------------------------------------------------------------------
// Проверка новых комментариев каждые 5 минут
//---------------------------------------------------------------------
cron.schedule('*/5 * * * *', () => {
    console.log('Checking for new comments...');
    checkForNewComments();
});

//---------------------------------------------------------------------
// Обработчик кнопки "Взять в работу"
//---------------------------------------------------------------------
bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
    try {
        // Обязательный ответ на колбэк
        await ctx.answerCallbackQuery();

        console.log('Нажали кнопку "Взять в работу":', ctx.match[1]);
        const taskId = ctx.match[1];
        const username = ctx.from.username;

        db.get('SELECT * FROM tasks WHERE id = ?', [taskId], async (err, task) => {
            if (err) {
                console.error('Ошибка при получении задачи из базы данных:', err);
                await ctx.reply('Произошла ошибка при обработке вашего запроса.');
                return;
            }

            if (!task) {
                try {
                    await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
                } catch (e) {
                    console.error('Ошибка при снятии кнопок:', e);
                }
                await ctx.reply('Задача не найдена.');
                return;
            }

            if (task.department === "Техническая поддержка") {
                let success = false;
                try {
                    success = await updateJiraTaskStatus(task.source, taskId, username);
                } catch (errUpdate) {
                    console.error('Ошибка при вызове updateJiraTaskStatus:', errUpdate);
                }

                console.log('updateJiraTaskStatus вернул:', success);

                if (success) {
                    const displayName = usernameMappings[ctx.from.username] || ctx.from.username;
                    const messageText = `Задача: ${task.id}\n` +
                        `Источник: ${task.source}\n` +
                        `Ссылка: ${getTaskUrl(task.source, task.id)}\n` +
                        `Описание: ${task.title}\n` +
                        `Приоритет: ${getPriorityEmoji(task.priority)}\n` +
                        `Отдел: ${task.department}\n` +
                        `Взял в работу: ${displayName}`;

                    // Убираем InlineKeyboard
                    try {
                        await ctx.editMessageText(messageText);
                    } catch (editErr) {
                        console.error('Ошибка при редактировании сообщения:', editErr);
                    }

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

//---------------------------------------------------------------------
// Функция для обновления статуса задачи в Jira
//---------------------------------------------------------------------
async function updateJiraTaskStatus(source, taskId, telegramUsername) {
    try {
        let transitionId;
        if (source === 'sxl') {
            transitionId = '221'; // Транзишен для sxl
        } else if (source === 'betone') {
            transitionId = '201'; // Транзишен для betone
        } else {
            console.error('Invalid source specified');
            return false;
        }

        const jiraUsername = jiraUserMappings[telegramUsername]?.[source];
        if (!jiraUsername) {
            console.error(`No Jira username mapping found for Telegram username: ${telegramUsername}`);
            return false;
        }

        // Назначаем исполнителя
        const assigneeUrl = `https://jira.${source}.team/rest/api/2/issue/${taskId}/assignee`;
        const pat = source === 'sxl' ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        const assigneeResponse = await axios.put(
            assigneeUrl,
            { name: jiraUsername },
            {
                headers: {
                    'Authorization': `Bearer ${pat}`,
                    'Content-Type': 'application/json'
                }
            }
        );

        if (assigneeResponse.status !== 204) {
            console.error(`Error assigning Jira task: ${assigneeResponse.status}`);
            return false;
        }

        // Делаем транзишен
        const transitionUrl = `https://jira.${source}.team/rest/api/2/issue/${taskId}/transitions`;
        const transitionResponse = await axios.post(
            transitionUrl,
            {
                transition: {
                    id: transitionId
                }
            },
            {
                headers: {
                    'Authorization': `Bearer ${pat}`,
                    'Content-Type': 'application/json'
                }
            }
        );

        return transitionResponse.status === 204;
    } catch (error) {
        console.error(`Error updating ${source} Jira task:`, error);
        return false;
    }
}

//---------------------------------------------------------------------
// Интеграция с Confluence для определения дежурного специалиста
//---------------------------------------------------------------------
// Допустим, что ID или пространство страницы - условное (надо уточнить реальный)
// Функция извлекает дежурного специалиста, сверяя сегодняшнюю дату и диапазон
// 1) Помощник: оборачивает confluence.getContentById(...) в Promise
function getContentByIdPromise(pageId, expandKey) {
    return new Promise((resolve, reject) => {
      // Подаём объект { expand: expandKey } 
      confluence.getContentById(pageId, { expand: expandKey }, (err, data) => {
        if (err) {
          console.error('Ошибка getContentById:', err);
          return reject(err);
        }
        // Выводим всё, что вернул Confluence. Посмотрите в логи, что там.
        console.log('Confluence getContentById data:', JSON.stringify(data, null, 2));
        resolve(data);
      });
    });
  }
  
  async function fetchDutyEngineer() {
    try {
      const pageId = '3539406';
      // Сперва попробуйте 'body.view'. Если не сработает – возможно, нужно 'body.storage'
      const response = await getContentByIdPromise(pageId, 'body.view');
  
      // Посмотрите, что реально есть в response.body
      console.log('Response body:', JSON.stringify(response.body, null, 2));
  
      // Попробуйте обращаться к response.body.view.value
      const html = response.body?.view?.value;
      if (!html) {
        console.log('Не обнаружено поле response.body.view.value');
        return 'Не найдено';
      }
  
      // Здесь парсим html
      const rowRegex = /<(?:tr|TR)[^>]*>\s*<td[^>]*>(\d+)<\/td>\s*<td[^>]*>(\d{2}\.\d{2}-\d{2}\.\d{2})<\/td>\s*<td[^>]*>([^<]+)<\/td>/g;
      let match;
      const schedule = [];
  
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
  
      // Проверяем, попадает ли сегодняшняя дата в один из интервалов
      const today = DateTime.now().setZone('Europe/Moscow');
      for (const item of schedule) {
        const [startStr, endStr] = item.range.split('-');
        const [startDay, startMonth] = startStr.split('.');
        const [endDay, endMonth] = endStr.split('.');
        const year = 2025;
  
        const startDate = DateTime.fromObject({
          year,
          month: Number(startMonth),
          day: Number(startDay),
          zone: 'Europe/Moscow'
        });
        const endDate = DateTime.fromObject({
          year,
          month: Number(endMonth),
          day: Number(endDay),
          zone: 'Europe/Moscow'
        });
  
        if (today >= startDate && today <= endDate) {
          return item.name;
        }
      }
  
      return 'Не найдено';
    } catch (error) {
      console.error('Ошибка получения данных из Confluence:', error);
      return 'Ошибка при запросе';
    }
  }

//---------------------------------------------------------------------
// Команда /duty для вывода текущего дежурного
//---------------------------------------------------------------------
bot.command('duty', async (ctx) => {
    const engineer = await fetchDutyEngineer(); // теперь работает!
    await ctx.reply(`Дежурный: ${engineer}`);
  });

//---------------------------------------------------------------------
// Расписание ночной и утренней смены
//---------------------------------------------------------------------
let interval = null;
let nightShiftCron = null;
let morningShiftCron = null;

// Запуск бота командой /start
bot.command('start', async (ctx) => {
    await ctx.reply('Привет! Каждую минуту я буду проверять новые задачи...');

    // Интервал проверки Jira-задач
    if (!interval) {
        interval = setInterval(async () => {
            console.log('Interval triggered. Fetching + Sending Jira tasks...');
            await fetchAndStoreJiraTasks();
            await sendJiraTasks(ctx);
            console.log('Jira tasks sent.');
        }, 60000);
    } else {
        await ctx.reply('Интервал уже запущен.');
    }

    // Расписание ночной и утренней смены
    if (!nightShiftCron) {
        // Ночная смена - в 01:00
        nightShiftCron = cron.schedule('0 1 * * *', async () => {
            await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброй ночи! Заполни тикет передачи смены.');
        }, {
            scheduled: true,
            timezone: 'Europe/Moscow'
        });

        // Утренняя смена - в 10:00
        if (!morningShiftCron) {
            morningShiftCron = cron.schedule('0 10 * * *', async () => {
                const engineer = await fetchDutyEngineer();
                await bot.api.sendMessage(
                    process.env.ADMIN_CHAT_ID,
                    `Доброе утро! Не забудь проверить задачи на сегодня: заполни тикет передачи смены.\nДежурный специалист: ${engineer}`
                );
            }, {
                scheduled: true,
                timezone: 'Europe/Moscow'
            });
        }

        nightShiftCron.start();
        morningShiftCron.start();
    }

    // Для отладки - проверка task_comments
    db.all('SELECT taskId FROM task_comments', [], async (err, rows) => {
        if (err) {
            console.error('Error fetching task comments:', err);
            return;
        }
        console.log(`Total task_comments in database: ${rows.length}`);
    });
});

// Запуск бота
bot.start();