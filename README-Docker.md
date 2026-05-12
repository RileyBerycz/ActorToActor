# Actor-to-Actor Game - Docker Setup

## Overview

This project is now fully containerized with Docker for easy local development and production deployment. The setup includes:

- **React frontend** with nginx web server
- **Python backend** for data processing
- **Multi-stage Docker builds** for optimization
- **Development and production environments**
- **Automated deployment scripts**

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- Git (for cloning/updating)
- 4GB+ available RAM
- 10GB+ available disk space

### Development Mode (Hot Reloading)

```bash
# Windows PowerShell
.\deploy.ps1 dev

# Linux/Mac
./deploy.sh dev
```

This starts:
- React dev server at http://localhost:3000
- Python backend at http://localhost:8000
- File watching for hot reloading

### Production Mode

```bash
# Windows PowerShell
.\deploy.ps1 prod

# Linux/Mac
./deploy.sh prod
```

This starts:
- Optimized React build served by nginx at http://localhost
- All Python services for data processing
- Background data synchronization

## Services Architecture

### Frontend (React)
- **Port**: 80 (production), 3000 (development)
- **Technology**: React 18, Create React App
- **Features**: Actor-to-Actor game interface, responsive design

### Backend Services
- **Data Processor**: Collects actor data from TMDB API
- **Database Sync**: Syncs local SQLite to Cloudflare D1
- **R2 Upload**: Manages file uploads to Cloudflare R2
- **Web Server**: nginx serving static files

### Data Flow
1. Python scripts collect actor data and store in SQLite
2. Database sync service uploads to Cloudflare D1
3. R2 service manages file uploads with versioning
4. React frontend queries Cloudflare Workers API

## Available Commands

### Build Commands
```bash
# Build Docker images
.\deploy.ps1 build        # Windows
./deploy.sh build         # Linux/Mac

# Build and start production
.\deploy.ps1 prod         # Windows
./deploy.sh prod          # Linux/Mac
```

### Development Commands
```bash
# Start development environment
.\deploy.ps1 dev          # Windows
./deploy.sh dev           # Linux/Mac

# View logs
docker-compose logs -f
docker-compose -f docker-compose.dev.yml logs -f
```

### Management Commands
```bash
# Stop all services
.\deploy.ps1 stop         # Windows
./deploy.sh stop          # Linux/Mac

# Clean up Docker resources
.\deploy.ps1 cleanup      # Windows
./deploy.sh cleanup       # Linux/Mac
```

## Environment Configuration

### Required Environment Variables

Create a `.env` file in the project root:

```env
# Cloudflare Configuration
CLOUDFLARE_API_TOKEN=your_api_token
CLOUDFLARE_ACCOUNT_ID=your_account_id

# TMDB API
TMDB_API_KEY=your_tmdb_api_key

# Optional: Development settings
NODE_ENV=development
REACT_APP_API_URL=http://localhost:8787
```

### Firebase Configuration

Place your Firebase service account key in:
```
firebase/actortoactor-c163f-firebase-adminsdk-fbsvc-[key].json
```

## Directory Structure

```
/
├── Dockerfile              # Multi-stage production build
├── Dockerfile.dev          # Development build
├── docker-compose.yml      # Production services
├── docker-compose.dev.yml  # Development services
├── nginx.conf              # Web server configuration
├── deploy.sh               # Linux/Mac deployment script
├── deploy.ps1              # Windows deployment script
├── requirements.txt        # Python dependencies
├── actor-game/             # React frontend source
├── data/                   # Generated data files
├── logs/                   # Application logs
├── databases/              # SQLite database files
└── firebase/               # Firebase configuration
```

## Troubleshooting

### Common Issues

1. **Docker not running**
   ```bash
   # Start Docker Desktop and wait for it to fully load
   docker info
   ```

2. **Port conflicts**
   ```bash
   # Check what's using port 80
   netstat -an | findstr :80      # Windows
   lsof -i :80                    # Linux/Mac
   ```

3. **Build failures**
   ```bash
   # Clean Docker cache
   .\deploy.ps1 cleanup           # Windows
   ./deploy.sh cleanup            # Linux/Mac
   ```

4. **Permission errors (Linux/Mac)**
   ```bash
   chmod +x deploy.sh
   sudo chown -R $USER:$USER data logs databases
   ```

### Debugging

View service logs:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f app
docker-compose logs -f data-processor
```

Access running containers:
```bash
# Main application container
docker-compose exec app sh

# Data processor container
docker-compose exec data-processor bash
```

## Production Deployment

### Local Ubuntu Server

1. Install Docker on Ubuntu:
   ```bash
   curl -fsSL https://get.docker.com -o get-docker.sh
   sudo sh get-docker.sh
   sudo usermod -aG docker $USER
   ```

2. Clone and deploy:
   ```bash
   git clone [your-repo-url]
   cd ActorToActor
   ./deploy.sh prod
   ```

### Cloudflare Integration

The application is designed to work with:
- **Cloudflare Workers** for API endpoints
- **Cloudflare D1** for database storage
- **Cloudflare R2** for file storage
- **Cloudflare Tunnels** for secure public access

Configure Cloudflare tunnels to expose your local server:
```bash
cloudflared tunnel create actor-to-actor
cloudflared tunnel route dns actor-to-actor yourdomain.com
cloudflared tunnel run actor-to-actor
```

## GitHub Actions Integration

The existing GitHub Actions workflows will work with the containerized setup. Key changes:

1. **Build step** now uses Docker
2. **Database operations** run in containers
3. **Deployment** can push Docker images to registries

Example workflow addition:
```yaml
- name: Build Docker image
  run: docker build -t actor-to-actor:${{ github.sha }} .

- name: Run data processing
  run: docker-compose run data-processor python update_actor_data.py
```

## Performance Notes

### Resource Usage
- **Development**: ~2GB RAM, moderate CPU
- **Production**: ~1GB RAM, low CPU (after initial data load)
- **Storage**: Grows with database size (typically 1-5GB)

### Optimization
- Images use Alpine Linux for smaller size
- Multi-stage builds reduce final image size
- nginx serves static files efficiently
- Python services can be scaled independently

## Next Steps

1. **Configure Cloudflare credentials** in `.env` file
2. **Set up TMDB API key** for data collection
3. **Configure Firebase** for authentication (if needed)
4. **Set up Cloudflare Tunnels** for public access
5. **Configure monitoring** and log aggregation

## Support

For issues with:
- **Docker setup**: Check Docker Desktop status and available resources
- **React frontend**: Check browser console and network tab
- **Python services**: Check container logs with `docker-compose logs`
- **Database issues**: Verify Cloudflare credentials and D1 setup
- **Performance**: Monitor resource usage with `docker stats`
