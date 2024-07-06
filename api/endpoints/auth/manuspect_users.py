import uuid
from fastapi import HTTPException
import requests
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from api.db_model import User

from pydantic import BaseModel, ValidationError
from typing import List, Any


class UserModel(BaseModel):
    id: int
    name: str
    email: str
    phone_number_code: int
    phone_number: int
    verification_status: int
    id_avatar: int
    roles: List[str]
    folders: List[Any]
    content: List[Any]


def fetch_user_data(token: str) -> UserModel:
    req_url = "https://manuspect.ru/auth/get_user"
    
    headers = {
        "Accept": "*/*",
        "Authorization": f'Bearer {token}'
    }
    
    response = requests.get(req_url, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Error fetching user data: {response.text}")
    
    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Failed to parse JSON response")
    
    try:
        user = UserModel(**data)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=f"Validation error: {e}")
    
    return user

async def auth_manuspect_user(
    auth_token: str, 
    session: AsyncSession
) -> User:
    manuspect_user = fetch_user_data(auth_token)
    result = await session.execute(
        select(User)
        .filter(
            User.hashed_password == str(manuspect_user.id),
        )
        .limit(1)  # Ограничиваем количество результатов до одного
    )
    user = result.scalars().first()
    
    if not user:
        user = User(id=uuid.uuid4(), email=manuspect_user.email, hashed_password=str(manuspect_user.id))
        session.add(user)
        await session.commit()
    else:
        print(manuspect_user.id, manuspect_user.name, manuspect_user.email)
    return user