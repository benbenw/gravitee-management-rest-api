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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.gravitee.definition.model.Proxy;
import io.gravitee.management.model.ApiEntity;
import io.gravitee.management.model.NewApiEntity;
import org.junit.Ignore;
import org.junit.Test;

import io.gravitee.common.http.HttpStatusCode;
import org.mockito.Mockito;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
public class ApisResourceTest extends AbstractResourceTest {

    protected String contextPath() {
        return "apis";
    }

    @Test
    public void testPostApi_nullApi() {
        final Response response = target().request().post(null);
        assertEquals(HttpStatusCode.BAD_REQUEST_400, response.getStatus());
    }

    @Test
    public void testPostApi_newApiWithoutPath() {
        final NewApiEntity apiEntity = new NewApiEntity();
        apiEntity.setName("My beautiful api");
        apiEntity.setVersion("v1");
        apiEntity.setDescription("my description");
        apiEntity.setProxy(new Proxy());

        ApiEntity returnedApi = new ApiEntity();
        returnedApi.setId("my-beautiful-api");
        doReturn(returnedApi).when(apiService).create(Mockito.any(NewApiEntity.class),
                Mockito.eq(USER_NAME));

        final Response response = target().request().post(Entity.json(apiEntity));
        assertEquals(HttpStatusCode.CREATED_201, response.getStatus());
    }
}
