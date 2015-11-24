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

import java.util.Map;

import org.springframework.cloud.dataflow.core.ArtifactCoordinates;
import org.springframework.cloud.dataflow.core.ModuleDefinition;
import org.springframework.cloud.dataflow.core.ModuleDeploymentId;
import org.springframework.cloud.dataflow.core.ModuleDeploymentRequest;
import org.springframework.cloud.dataflow.module.ModuleStatus;
import org.springframework.cloud.dataflow.module.deployer.ModuleDeployer;

public class YarnTaskModuleDeployer implements ModuleDeployer {

	private final YarnCloudAppService yarnCloudAppService;

	public YarnTaskModuleDeployer(YarnCloudAppService yarnCloudAppService) {
		this.yarnCloudAppService = yarnCloudAppService;
	}

	@Override
	public ModuleDeploymentId deploy(ModuleDeploymentRequest request) {
		int count = request.getCount();
		ArtifactCoordinates coordinates = request.getCoordinates();
		ModuleDefinition definition = request.getDefinition();
		ModuleDeploymentId id = ModuleDeploymentId.fromModuleDefinition(definition);
		String module = coordinates.toString();
		Map<String, String> definitionParameters = definition.getParameters();
		Map<String, String> deploymentProperties = request.getDeploymentProperties();

		yarnCloudAppService.pushApplication("app");
		yarnCloudAppService.submitApplication("app");

		return id;
	}

	@Override
	public void undeploy(ModuleDeploymentId id) {
	}

	@Override
	public ModuleStatus status(ModuleDeploymentId id) {
		return null;
	}

	@Override
	public Map<ModuleDeploymentId, ModuleStatus> status() {
		return null;
	}

}
