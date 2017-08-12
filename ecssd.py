import sys, docker, json, logging

logging.basicConfig()
log = logging.getLogger('ECSServiceRegistrator')
log.setLevel('WARN')

class ECSServiceRegistrator:

	def __init__(self):
		self.client  = docker.from_env()

	def get_environment_of_container(self, container):
		result = {}
		env = container.attrs['Config']['Env']
		for e in env:
			parameter = e.split('=', 1)
			result[parameter[0]] = parameter[1]
		return result

	def register_service(self, container_id, container, port, name);
		log.warn('registering port %s of container %s with service name %s' % (port, container_id, name))
		
	def container_started(self, container_id, event):
		try:
			container = self.client.containers.get(container_id)
			env = self.get_environment_of_container(container)
			ports = container.attrs['NetworkSettings']['Ports'] 
			for p in ports:
				port = p.split('/')
				service_name = 'SERVICE_%s_NAME' % port[0]
				if service_name in env:
					self.register_service(container_id, container, port, name)
				elif 'SERVICE_NAME' in env and len(ports) == 1:
					self.register_service(container_id, container, port, name)
				else:
					log.warn('port %s of container %s is exposed but ignored' % (p, container_id))
		except docker.errors.NotFound as e:
			log.error('container %s does not exist.' % container_id)


	def container_died(self, id, event):
		pass
		
	def process_events(self):
		for e in self.client.events():
			event = json.loads(e)
			if event['Type'] == 'container': 
				if event['status'] == 'start':
					self.container_started(event['id'], event)
				elif event['status'] == 'die':
					self.container_died(event['id'], event)
				else:
					pass # boring...
			else:
				pass # boring...

	def main(self):
		self.process_events()

if __name__ == '__main__':
	e = ECSServiceRegistrator()
	e.main()
