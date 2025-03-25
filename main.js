const { app, BrowserWindow } = require('electron');
const path = require('path');
const { spawn } = require('child_process');

function createWindow() {
    const win = new BrowserWindow({
        width: 1600,
        height: 900,
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
        }
    });

    // Load index.html
    win.loadFile(path.join(__dirname, 'dist', 'pistachio-ui', 'browser', 'index.html'));
    win.setMenu(null);
}

// Starting Flask in a separate process
const pythonPath = path.join(__dirname, '.venv', 'Scripts', 'python.exe');
const flaskProcess = spawn(pythonPath, [path.join(__dirname, 'main.py')]);

flaskProcess.stdout.on('data', (data) => {
    console.log(`stdout: ${data}`);

});

flaskProcess.stderr.on('data', (data) => {
    console.error(`stderr: ${data}`);
});

flaskProcess.on('close', (code) => {
    console.log(`Flask server exited with code ${code}`);
});

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
    if (process.platform !== 'darwin') {
        app.quit();
    }
});
