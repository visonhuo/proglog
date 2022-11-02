function(request) {
	local statefulSet = request.object,
	local labelKey = statefulSet.metadata.annotations["service-per-pod-label"],
	local ports = statefulSet.metadata.annotations["service-per-pod-ports"],
	attachments: [
		{
			apiVersion: "v1",
			kind: "Service",
			metadata: {
				name: statefulSet.metadata.name + "-" + index,
				labels: {app: "service-per-pod"}
			},
			spec: {
				type: "LoadBalance",
				selector: {
					[labelKey]: statefulSet.metadata.name + "-" + index
				},
				ports: [
					{
						local parts = std.split(portnums, ":"),
						port: std.parseInt(parts[0]),
						targetPort: std.parseInt(parts[1]),
					}
					for portnums in std.split(ports, ",")
				]
			}
		}
		for index in std.range(0, statefulSet.spec.replicas - 1)
	]
}