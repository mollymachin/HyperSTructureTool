#!/usr/bin/env node
// For elegant startup allowing user to run "npm start" from root

const { execSync } = require('child_process');
const fs = require('fs');

function getProcessesOnPort(port) {
  try {
    const output = execSync(`lsof -i :${port} -t`, { encoding: 'utf8' });
    return output.trim().split('\n').filter(pid => pid);
  } catch (error) {
    return [];
  }
}

function getProcessInfo(pid) {
  try {
    const output = execSync(`ps -p ${pid} -o pid,ppid,command`, { encoding: 'utf8' });
    const lines = output.trim().split('\n');
    if (lines.length > 1) {
      const parts = lines[1].trim().split(/\s+/);
      return {
        pid: parts[0],
        ppid: parts[1],
        command: parts.slice(2).join(' ')
      };
    }
  } catch (error) {
    // Process might have already been killed
  }
  return null;
}

function isOurService(processInfo) {
  if (!processInfo) return false;
  
  const command = processInfo.command.toLowerCase();
  
  // Check for our specific services
  const isReactDevServer = command.includes('react-scripts') || command.includes('webpack-dev-server');
  const isFastAPIServer = command.includes('python') && (command.includes('main.py') || command.includes('uvicorn'));
  const isNodeDevServer = command.includes('node') && command.includes('react-scripts');
  
  return isReactDevServer || isFastAPIServer || isNodeDevServer;
}

function killProcess(pid, force = false) {
  try {
    const signal = force ? '-9' : '-TERM';
    execSync(`kill ${signal} ${pid}`, { stdio: 'pipe' });
    return true;
  } catch (error) {
    return false;
  }
}

function cleanupPort(port, serviceName) {
  console.log(`🔍 Checking port ${port} for ${serviceName}...`);
  
  const pids = getProcessesOnPort(port);
  
  if (pids.length === 0) {
    console.log(`✅ Port ${port} is free`);
    return;
  }
  
  let killedCount = 0;
  
  for (const pid of pids) {
    const processInfo = getProcessInfo(pid);
    
    if (isOurService(processInfo)) {
      console.log(`🔄 Found our ${serviceName} process (PID: ${pid})`);
      
      // Try graceful shutdown first
      if (killProcess(pid, false)) {
        console.log(`✅ Gracefully stopped ${serviceName} (PID: ${pid})`);
        killedCount++;
      } else {
        // Force kill if graceful failed
        if (killProcess(pid, true)) {
          console.log(`⚠️  Force killed ${serviceName} (PID: ${pid})`);
          killedCount++;
        } else {
          console.log(`❌ Failed to kill ${serviceName} (PID: ${pid})`);
        }
      }
    } else {
      console.log(`⚠️  Found other process on port ${port} (PID: ${pid}) - leaving it alone`);
      console.log(`   Command: ${processInfo?.command || 'unknown'}`);
    }
  }
  
  if (killedCount > 0) {
    console.log(`✅ Cleaned up ${killedCount} ${serviceName} process(es) on port ${port}`);
  }
}

// Main cleanup
console.log('🧹 Starting safe cleanup...\n');

cleanupPort(3000, 'React development server');
cleanupPort(8000, 'FastAPI backend server');

console.log('\n✨ Cleanup complete!'); 