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
package io.gravitee.management.rest.resource;

import io.gravitee.management.model.MemberEntity;
import io.gravitee.management.model.NewUserEntity;
import io.gravitee.management.rest.resource.param.MembershipTypeParam;
import io.gravitee.management.service.ApiService;
import io.gravitee.management.service.UserService;
import io.gravitee.management.service.exceptions.UserNotFoundException;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import io.gravitee.common.http.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
public class ApiMembersResource {

    @Inject
    private ApiService apiService;

    @Inject
    private UserService userService;

    @PathParam("api")
    private String api;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Set<MemberEntity> members() {
        // Check that the API exists
        apiService.findById(api);

        return apiService.getMembers(api, null).stream()
                .sorted((o1, o2) -> o1.getUser().compareTo(o2.getUser()))
                .collect(Collectors.toSet());
    }

    @POST
    public Response save(
            @NotNull @QueryParam("user") String username,
            @NotNull @QueryParam("type") MembershipTypeParam membershipType) {
        // Check that the API exists
        apiService.findById(api);

        if (membershipType.getValue() == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        try {
            userService.findByName(username);
        } catch (UserNotFoundException unfe) {
        	// Create user with only user name data
        	// The others information will be updated during its first connection
        	NewUserEntity user = new NewUserEntity();
        	user.setUsername(username);
        	user.setPassword(StringUtils.EMPTY);
        	user.setRoles(new HashSet<String>(Arrays.asList(new String[]{"ROLE_USER"})));
        	userService.create(user);
        }

        apiService.addOrUpdateMember(api, username, membershipType.getValue());

        return Response.created(URI.create("/apis/" + api + "/members/" + username)).build();
    }

    @DELETE
    public Response delete(@NotNull @QueryParam("user") String username) {
        // Check that the API exists
        apiService.findById(api);

        try {
            userService.findByName(username);
        } catch (UserNotFoundException unfe) {
            return Response.status(Response.Status.BAD_REQUEST).entity(unfe.getMessage()).build();
        }

        apiService.deleteMember(api, username);

        return Response.ok().build();
    }
}
