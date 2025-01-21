use std::fs;
use tokio::sync::Semaphore;
use tokio::task;
use sled::Db;
use reqwest::Client;
use std::sync::Arc;
use serde::Deserialize;
use anyhow::{Result, Context};

// Определяем структуру для входных данных
#[derive(Deserialize)]
struct InputData {
    urls: Vec<String>, // Список URL, которые нужно обработать
}

#[tokio::main]
async fn main() -> Result<()> {
    // Загружаем входной JSON-файл
    let input_file = "input.json";
    let file_content = fs::read_to_string(input_file)
        .with_context(|| format!("Не удалось прочитать файл: {}", input_file))?;
    
    // Парсим JSON-файл в структуру InputData
    let input_data: InputData = serde_json::from_str(&file_content)
        .with_context(|| "Не удалось разобрать JSON")?;

    let urls = input_data.urls; // Извлекаем список URL

    // Создаем in-memory базу данных с использованием sled
    let db = sled::Config::new().temporary(true).open()?;

    // Создаем асинхронный HTTP-клиент
    let client = Client::new();

    // Устанавливаем ограничение на количество одновременных задач
    let max_concurrent_tasks = 10;
    let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));

    // Обрабатываем каждый URL асинхронно
    let tasks: Vec<_> = urls.into_iter().map(|url| {
        let client = client.clone();
        let db = db.clone();
        let semaphore = semaphore.clone();

        // Создаем задачу для обработки URL
        task::spawn(async move {
            let permit = semaphore.acquire().await.unwrap(); // Захватываем слот для выполнения задачи

            // Выполняем обработку URL
            match process_url(&client, &url).await {
                Ok(result) => {
                    // Сохраняем результат в базу данных
                    save_to_db(&db, &url, &result).unwrap_or_else(|e| {
                        eprintln!("Не удалось сохранить результат для {}: {}", url, e);
                    });
                }
                Err(e) => {
                    // Логируем ошибку обработки URL
                    eprintln!("Ошибка обработки {}: {}", url, e);
                }
            }

            drop(permit); // Освобождаем слот для следующей задачи
        })
    }).collect();

    // Ожидаем завершения всех задач
    for task in tasks {
        task.await.unwrap();
    }

    // Выводим результаты из базы данных
    display_results(&db)?;

    Ok(())
}

// Асинхронная функция для обработки URL
async fn process_url(client: &Client, url: &str) -> Result<String> {
    // Выполняем HTTP-запрос к указанному URL
    let response = client.get(url).send().await?.text().await?;
    // Возвращаем длину содержимого страницы как строку
    Ok(format!("Длина: {}", response.len()))
}

// Функция для сохранения результата в базу данных
fn save_to_db(db: &Db, url: &str, result: &str) -> Result<()> {
    db.insert(url, result.as_bytes())?; // Сохраняем URL и результат в виде пары ключ-значение
    Ok(())
}

// Функция для вывода результатов из базы данных
fn display_results(db: &Db) -> Result<()> {
    println!("Результаты:");
    for result in db.iter() {
        let (key, value) = result?; // Извлекаем ключ (URL) и значение (результат)
        let url = String::from_utf8(key.to_vec())?;
        let result = String::from_utf8(value.to_vec())?;
        println!("{} -> {}", url, result); // Выводим результаты в консоль
    }
    Ok(())
}
