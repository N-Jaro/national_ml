import time
import ee

class TaskManager:
    def __init__(self):
        self.all_tasks = []

    def add_task(self, task, description, folder=None):
        """Add a task to the internal task list."""
        self.all_tasks.append({
            'task': task,
            'description': description,
            'folder': folder,
            'state': 'PENDING',
            'created': time.strftime('%Y-%m-%d %H:%M:%S')
        })

    def start_all_tasks(self):
        """Start all tasks in the task list."""
        print(f"\n--- Starting {len(self.all_tasks)} tasks ---")
        for item in self.all_tasks:
            try:
                item['task'].start()
                print(f"Started task: {item['description']} (ID: {item['task'].id})")
            except Exception as e:
                print(f"Failed to start task: {item['description']} | Error: {e}")

    def monitor_tasks(self, poll_interval=60):
        """Monitor the status of all launched tasks until completion."""
        print("\n--- Monitoring Tasks ---")
        while True:
            active_tasks = []
            all_done = True
            for item in self.all_tasks:
                if item['state'] not in ['COMPLETED', 'FAILED']:
                    try:
                        status = item['task'].status()
                        state = status['state']
                        item['state'] = state
                        if state in ['RUNNING', 'READY']:
                            active_tasks.append(item['description'])
                            all_done = False
                        elif state == 'FAILED':
                            print(f"!!! Task FAILED: {item['description']} | Error: {status.get('error_message', 'Unknown error')}")
                    except ee.ee_exception.EEException as e:
                        if '503' in str(e) or 'unavailable' in str(e):
                            print(f"Warning: Temporary GEE error checking task '{item['description']}'. Retrying...")
                            active_tasks.append(item['description'])
                            all_done = False
                        else:
                            print(f"!!! Task FAILED due to unknown error: {item['description']} | Error: {e}")
                            item['state'] = 'FAILED'

            if all_done:
                print("\nAll tasks finished!")
                break

            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Ongoing tasks ({len(active_tasks)}): {', '.join(active_tasks[:3])}...")
            time.sleep(poll_interval)

    def get_all_tasks(self):
        return self.all_tasks

    def get_task_summary(self):
        summary = {'PENDING': 0, 'RUNNING': 0, 'READY': 0, 'COMPLETED': 0, 'FAILED': 0}
        for item in self.all_tasks:
            state = item.get('state', 'PENDING')
            if state in summary:
                summary[state] += 1
            else:
                summary[state] = 1
        return summary
