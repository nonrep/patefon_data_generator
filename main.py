import psycopg2
from faker import Faker
import random
import time
import uuid

# Импортируйте параметры подключения из вашего файла конфигурации
from config import host, user, password, db_name, port

# Указание порта для подключения
fake = Faker('ru_RU')

def generate_users(n):
    users = []
    usernames = set()  # Для хранения уникальных юзернеймов

    while len(users) < n:
        username = fake.user_name()
        
        # Проверяем, уникален ли username
        if username not in usernames:
            usernames.add(username)  # Добавляем новый username в множество
            
            users.append(
                (
                    str(uuid.uuid4()),
                    username,
                    fake.password(),
                    fake.name(),
                    fake.text(max_nb_chars=200),
                    fake.date_of_birth(minimum_age=18, maximum_age=75).strftime('%Y-%m-%d'),
                    fake.image_url(),
                )
            )

    return users


def generate_chats(n):
    chats = []
    for _ in range(n):
        chats.append(
            (
                str(uuid.uuid4()),
                fake.company(),
                2,  # Важно указать верные значения для chat_type_id
                fake.image_url(),
            )
        )
    return chats

def generate_messages(n, user_ids, chat_id):
    messages = []
    for _ in range(n):
        messages.append(
            (
                str(uuid.uuid4()),
                chat_id,
                random.choice(user_ids),  # Случайный пользователь из списка
                fake.text(max_nb_chars=200),
                fake.date_time_this_decade().isoformat(),
                random.choice([True, False])  # is_redacted
            )
        )
    return messages

# Главное тело программы
start_time = time.time()

# 1x users
# 2x chat
# 20x messages
dataMultiplier = 1_000

try:
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name,
        port=port
    )
    connection.autocommit = True

    with connection.cursor() as cursor:
        # Генерация пользователей
        print('[INFO] генерация пользователей')
        user_data = generate_users(dataMultiplier)
        cursor.executemany("""
            INSERT INTO public.user (id, username, password, name, bio, birthdate, avatar_path, version) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
        """, user_data)

        print('[INFO] пользователи созданы ', time.time() - start_time)


        # Получаем созданные ID пользователей
        cursor.execute("SELECT id FROM public.user")
        user_ids = [row[0] for row in cursor.fetchall()]

        print('[INFO] пользователи получены')

        # Генерация чатов
        print('[INFO] генерация чатов')
        chat_data = generate_chats(2 * dataMultiplier)  # Сгенерировать 100,000 чатов
        cursor.executemany("""
            INSERT INTO public.chat (id, chat_name, chat_type_id, avatar_path) 
            VALUES (%s, %s, %s, %s)
        """, chat_data)

        print('[INFO] чаты созданы ', time.time() - start_time)

        # Получаем созданные ID чатов
        cursor.execute("SELECT id FROM public.chat")
        chat_ids = [row[0] for row in cursor.fetchall()]

        # Добавление пользователей в чаты
        i = 0

        print(len(user_ids))
        print(len(chat_ids))

        for chat_id in chat_ids:
            batch_size = 5

            if len(user_ids) < i + batch_size:
                print('[ERR] выход за предел массива юзеров')
                i = 0

            selected_users = random.sample(user_ids, batch_size)

            if len(selected_users) < batch_size:
                print('[ERR] юров осталось меньше 5')
                break

            
            for user_id in selected_users:
                cursor.execute("""
                    INSERT INTO public.chat_user (id, user_role_id, chat_id, user_id) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chat_id, user_id) DO NOTHING
                """, (str(uuid.uuid4()), 2, chat_id, user_id))

            # Генерация сообщений для каждого чата
            message_data = generate_messages(10, selected_users, chat_id)

            cursor.executemany("""
                INSERT INTO public.message (id, chat_id, author_id, message, sent_at, is_redacted) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, message_data)

            i += 1
        
        print('[INFO] сообщения созданы ', time.time() - start_time)


except Exception as ex:
    print("[INFO] Error while working with PostgreSQL", ex)

finally:
    if connection:
        connection.close()
        print('[INFO] PostgreSQL connection closed')

end_time = time.time()
execution_time = end_time - start_time
print(f"Время выполнения программы: {execution_time} секунд")
