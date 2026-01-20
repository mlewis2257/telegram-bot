const TelegramBot = require("node-telegram-bot-api");
const https = require("https");
https.globalAgent = new https.Agent({ rejectUnauthorized: false });

// replace the value below with the Telegram token you receive from @BotFather
const token = "6759685607:AAE5EV6du2laJK3N7g1QpCgyWRLj2SyEQtc";

// Create a bot that uses 'polling' to fetch new updates
const bot = new TelegramBot(token, { polling: true });

// Matches "/echo [whatever]"
// bot.onText(/\/echo (.+)/, (msg, match) => {
//   // 'msg' is the received Message from Telegram
//   // 'match' is the result of executing the regexp above on the text content
//   // of the message

//   const chatId = msg.chat.id;
//   const resp = match[1]; // the captured "whatever"

//   // send back the matched "whatever" to the chat
//   bot.sendMessage(chatId, resp);
// });

// Listen for any kind of message. There are different kinds of
// messages.
bot.on("message", (msg) => {
  const welcome = "Welcome to the Solana Alpha Bot";
  if (msg.text.toString().toLowerCase().indexOf(welcome) === 0) {
    bot.sendMessage(msg.chat.id, "Hello user glad to have you here");
  }
  const bye = "See you later";
  if (msg.text.toString().toLowerCase().indexOf(bye) === 0) {
    bot.sendMessage(msg.chat.id, "Hope to see you again, later");
  }

  // send a message to the chat acknowledging receipt of their message
  bot.sendMessage(chatId, "Received your message");
});
