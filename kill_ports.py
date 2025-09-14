"""
Kill processes using MetroMind ports
"""
import subprocess
import sys

def kill_port_processes():
    """Kill processes that might be using MetroMind ports"""
    ports = [8010, 8011, 8012, 8013, 8014, 8015, 8016, 8017, 8018, 8019, 8020, 8021]
    
    for port in ports:
        try:
            # For Windows
            cmd = f'netstat -ano | findstr :{port}'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if f':{port}' in line and 'LISTENING' in line:
                        parts = line.split()
                        if parts:
                            pid = parts[-1]
                            try:
                                subprocess.run(f'taskkill /PID {pid} /F', shell=True)
                                print(f"Killed process {pid} using port {port}")
                            except:
                                pass
        except:
            pass

if __name__ == "__main__":
    kill_port_processes()
    print("Port cleanup complete")