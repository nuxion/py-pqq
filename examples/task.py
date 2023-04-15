from pydantic import BaseModel


class Person(BaseModel):
    name: str


def cpu_bound_task(p: Person):
    print(f"Hello {p.name}")


async def io_bound_task(p: Person):
    print(f"Hello {p.name}")
