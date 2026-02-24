from app.database import engine, Base
from app.models import User, TypologyResult, ElectedOfficial, Content

print("Creating tables...")
Base.metadata.create_all(bind=engine)
print("All tables created successfully!")
