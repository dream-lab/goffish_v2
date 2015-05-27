/**
 *
 */
package edu.usc.goffish.gofs.slice;


import java.util.Collection;
import java.util.Collections;

import edu.usc.goffish.gofs.ISubgraphTemplate;
import edu.usc.goffish.gofs.ITemplateVertex;
import edu.usc.goffish.gofs.PropertySet;
import edu.usc.goffish.gofs.partition.BaseSubgraph;
import edu.usc.goffish.gofs.partition.TemplateEdge;
import edu.usc.goffish.gofs.partition.TemplateVertex;
import edu.usc.goffish.gofs.util.IterableUtils;

/**
 * Replace this line with a description of what this class does.
 * Feel free to be verbose and descriptive for key classes.
 *
 * @author Neel Choudhury [mailto:neel@ssl.serc.in]
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TextSliceSubgraph extends BaseSubgraph {
	
	private Collection<? extends ITemplateVertex> _cachedRemoteVertices;

	public TextSliceSubgraph(long id, ISubgraphTemplate<
			TemplateVertex, TemplateEdge> template, PropertySet vertexProperties, PropertySet edgeProperties) {
		super(id, template, vertexProperties, edgeProperties);
		

	}
	

	
	@Override
	public Collection<? extends ITemplateVertex> remoteVertices() {
		if (_cachedRemoteVertices == null) {
			_cachedRemoteVertices = Collections.unmodifiableCollection(IterableUtils.toList(super.remoteVertices()));
		}
		
		return _cachedRemoteVertices;
	}

	@Override
	public int numRemoteVertices() {
		return remoteVertices().size();
	}
}
