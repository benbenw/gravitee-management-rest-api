/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.management.service;

import io.gravitee.management.model.*;

import java.util.Set;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
public interface ApplicationService {

    ApplicationEntity findById(String applicationId);

    Set<ApplicationEntity> findByUser(String username);

    ApplicationEntity create(NewApplicationEntity application, String username);

    ApplicationEntity update(String applicationId, UpdateApplicationEntity application);

    void delete(String applicationId);

    Set<ApplicationEntity> findByApi(String applicationId);

    Set<MemberEntity> getMembers(String applicationId, MembershipType membershipType);

    MemberEntity getMember(String applicationId, String username);

    void addOrUpdateMember(String applicationId, String username, MembershipType membershipType);

    void deleteMember(String applicationId, String username);
}
