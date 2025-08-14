import matplotlib.pyplot as plt
import string
import operator
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests


def get_text(url):
    """
    Завантажує текст із вказаного URL.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Помилка: Не вдалося отримати текст з {url}. Деталі: {e}")
        return None


def remove_punctuation(text):
    """
    Видаляє розділові знаки з тексту.
    """
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word):
    """
    Функція Mapper: приймає слово і повертає пару (слово, 1).
    """
    return word, 1


def shuffle_function(mapped_values):
    """
    Групує ідентичні слова разом для подальшої обробки.
    """
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    """
    Функція Reducer: підсумовує кількість входжень для кожного слова.
    """
    key, values = key_values
    return key, sum(values)


def map_reduce(text, search_words=None):
    """
    Основна функція MapReduce для підрахунку слів.
    """
    text = remove_punctuation(text)
    words = text.split()
    words = [word.lower() for word in words]  # Переводимо слова в нижній регістр

    if search_words:
        words = [word for word in words if word in search_words]

    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    shuffled_values = shuffle_function(mapped_values)

    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


def visualize_top_words(word_frequencies, quantity=10):
    """
    Візуалізує топ-N слів за частотою використання.

    Аргументи:
    word_frequencies (dict): Словник, де ключ - слово, значення - його частота.
    quantity (int): Кількість слів для візуалізації.
    """
    sorted_words = sorted(
        word_frequencies.items(), key=operator.itemgetter(1), reverse=True
    )

    top_words = sorted_words[:quantity]

    words = [word for word, count in top_words]
    counts = [count for word, count in top_words]

    plt.figure(figsize=(12, 7))
    plt.bar(words, counts, color="skyblue")
    plt.xlabel("Слово", fontsize=12)
    plt.ylabel("Частота", fontsize=12)
    plt.title(f"Топ {quantity} слів за частотою використання", fontsize=14)
    plt.xticks(rotation=45, ha="right", fontsize=10)
    plt.yticks(fontsize=10)
    plt.tight_layout()
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    # Зберігаємо графік у файл, оскільки інтерактивне відображення може бути недоступним
    plt.savefig("top_words_frequency.png")


if __name__ == "__main__":
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)
    if text:
        # Приклад використання: можна розкоментувати, щоб шукати конкретні слова
        # search_words = ["war", "peace", "love"]
        result = map_reduce(text)

        print("Результат підрахунку слів:", result)

        top_words_quantity = 10

        visualize_top_words(result, top_words_quantity)
        print("Графік збережено у файл 'top_words_frequency.png'")
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")
