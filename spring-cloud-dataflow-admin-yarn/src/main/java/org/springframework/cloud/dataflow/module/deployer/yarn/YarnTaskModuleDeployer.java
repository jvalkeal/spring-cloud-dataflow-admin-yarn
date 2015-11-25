/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.module.deployer.yarn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.dataflow.core.ArtifactCoordinates;
import org.springframework.cloud.dataflow.core.ModuleDefinition;
import org.springframework.cloud.dataflow.core.ModuleDeploymentId;
import org.springframework.cloud.dataflow.core.ModuleDeploymentRequest;
import org.springframework.cloud.dataflow.module.ModuleStatus;
import org.springframework.cloud.dataflow.module.deployer.ModuleDeployer;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppType;

public class YarnTaskModuleDeployer implements ModuleDeployer {

	private static final Logger logger = LoggerFactory.getLogger(YarnTaskModuleDeployer.class);
	private final YarnCloudAppService yarnCloudAppService;

	public YarnTaskModuleDeployer(YarnCloudAppService yarnCloudAppService) {
		this.yarnCloudAppService = yarnCloudAppService;
	}

	@Override
	public ModuleDeploymentId deploy(ModuleDeploymentRequest request) {
		ArtifactCoordinates coordinates = request.getCoordinates();
		ModuleDefinition definition = request.getDefinition();
		ModuleDeploymentId id = ModuleDeploymentId.fromModuleDefinition(definition);
		String module = coordinates.toString();
		Map<String, String> definitionParameters = definition.getParameters();
		Map<String, String> deploymentProperties = request.getDeploymentProperties();

		logger.info("deploying request for definition: " + definition);
		logger.info("deploying module: " + module);
		logger.info("definitionParameters: " + definitionParameters);
		logger.info("deploymentProperties: " + deploymentProperties);

		ArrayList<String> contextRunArgs = new ArrayList<String>();
		contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--dataflow.module.coordinates=" + module);
		for (Entry<String, String> entry : definitionParameters.entrySet()) {
			contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--dataflow.module.parameters." + entry.getKey() + ".=" + entry.getValue());
		}

		yarnCloudAppService.pushApplication("app", CloudAppType.TASK);
		yarnCloudAppService.submitApplication("app", CloudAppType.TASK, contextRunArgs);

		return id;
	}

	@Override
	public void undeploy(ModuleDeploymentId id) {
		// kill yarn app if we find matching instance
	}

	@Override
	public ModuleStatus status(ModuleDeploymentId id) {
		return ModuleStatus.of(id)
				.with(new YarnModuleInstanceStatus(id.toString(), false,
						Collections.<String, String>emptyMap()))
				.build();
	}

	@Override
	public Map<ModuleDeploymentId, ModuleStatus> status() {
		return null;
	}

}
