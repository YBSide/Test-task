'''
Проблема в том, что функции в списке handlers обращаются к переменной step, которая изменяется.
Поэтому на последней итерации цикла, когда step=4, все функции списка обращаются к переменной step=4.
Исправить ошибку можно так:
'''

from typing import Any, Callable, List

def create_handlers(callback: Callable[..., Any]) -> List[Callable[..., Any]]:
    handlers: List[Callable[..., Any]] = []
    for step in range(5):
        handlers.append(lambda step=step: callback(step))
    return handlers

def execute_handlers(handlers: Callable[..., Any]) -> None:
    for handler in handlers:
        handler()
