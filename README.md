# Telegram OTP Bot

A Telegram bot for fetching OTPs and managing phone numbers from a panel.

## Features

-   Fetches SMS and OTPs from a panel via API.
-   Provides an interface to get phone numbers (random or by country).
-   Masks phone numbers in group chats for privacy.
-   Admin commands to broadcast messages and add numbers.
-   Terminal-based operation, suitable for servers and Termux.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

Before running the bot, you need to configure the following variables in `app.py`:

-   `TELEGRAM_BOT_TOKEN`: Your Telegram bot token.
-   `GROUP_ID`: The ID of the Telegram group where the bot will send messages.
-   `CHANNEL_ID`: The ID of the Telegram channel for subscription checks.
-   `ADMIN_ID`: Your Telegram user ID.
-   `ADMIN_USERNAME`: Your Telegram username.
-   `PHPSESSID`: Your session ID for the panel.

## Usage

To run the bot, execute the following command in your terminal:

```bash
python app.py
```

The bot will start and log its activity to `bot_error.log`. You can interact with the bot on Telegram using the commands and buttons it provides.

To stop the bot, press `Ctrl+C` in the terminal.
