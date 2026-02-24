from pydantic import BaseModel, EmailStr

# What the user sends to register
class UserCreate(BaseModel):
    email: EmailStr
    password: str
    full_name: str | None = None
    zip_code: str | None = None

# What the user sends to login
class UserLogin(BaseModel):
    email: EmailStr
    password: str

# What we send back (never includes password)
class UserResponse(BaseModel):
    id: int
    email: str
    full_name: str | None
    zip_code: str | None
    is_active: bool
    is_verified: bool

    class Config:
        from_attributes = True

# Token response after login
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
