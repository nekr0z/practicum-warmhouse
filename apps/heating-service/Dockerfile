FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY main.py .

# Expose the port the app runs on
EXPOSE 8082

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8082"]
