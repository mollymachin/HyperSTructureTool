import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import './HyperstructureVisualisation.css';

interface HyperstructureVisualisationProps {
  data: any;
  isProcessing: boolean;
  showStateCausality: boolean;
}

interface Node {
  id: string;
  type: 'entity' | 'relation' | 'context' | 'state_change';
  label: string;
  x: number;
  y: number;
  fx?: number | null;
  fy?: number | null;
  isSubject?: boolean;
  isObject?: boolean;
  stateAffectedKey?: string; // for state nodes only
}

interface Link {
  source: string;
  target: string;
  directed?: boolean;
  colour?: string;
  color?: string;
  dashed?: boolean;
  label?: string;
  labelColour?: string;
  labelColor?: string;
}

// Compute node radius based on the longest line of the label
const getNodeRadius = (label: string | undefined): number => {
  const safeLabel = label || '';
  const lines = safeLabel.split('\n');
  const longestLineLength = lines.reduce((max, line) => Math.max(max, line.length), 0);
  const base = 20 + longestLineLength * 2;
  return Math.max(30, Math.min(60, base));
};

// Compute the boundary point on a node in the direction of (tx, ty)
// Supports circle (entity/context) and diamond (relation)
const computeBoundaryPoint = (node: any, tx: number, ty: number): { x: number, y: number } => {
  const cx = node.x as number;
  const cy = node.y as number;
  const dx = tx - cx;
  const dy = ty - cy;
  const dist = Math.sqrt(dx * dx + dy * dy) || 1;
  const ux = dx / dist;
  const uy = dy / dist;
  const r = getNodeRadius(node.label);

  if (node.type === 'relation') {
    // Diamond defined by |x| + |y| <= r relative to centre
    const absUx = Math.abs(ux);
    const absUy = Math.abs(uy);
    const scale = r / ((absUx + absUy) || 1);
    return { x: cx + ux * scale, y: cy + uy * scale };
  }

  // Circle fallback
  return { x: cx + ux * r, y: cy + uy * r };
};

// Get fill and stroke colours for node consistently for enter/update
const getNodeColours = (d: any): { fill: string, stroke: string } => {
  if (d.type === 'state_change') {
    // Even lighter inner fill, keep darker stroke for contrast
    return { fill: 'rgba(147, 112, 219, 0.45)', stroke: 'rgba(120, 90, 190, 0.9)' };
  }
  if (d.type === 'context') {
    return { fill: 'rgba(255, 245, 157, 0.85)', stroke: 'rgba(230, 200, 80, 0.7)' };
  }
  if (d.type === 'relation') {
    return { fill: 'rgba(173, 216, 230, 0.85)', stroke: 'rgba(100, 170, 200, 0.7)' };
  }
  // entity
  if (d.isSubject && d.isObject) {
    return { fill: 'rgba(255, 200, 120, 0.95)', stroke: 'rgba(220, 150, 80, 0.8)' };
  }
  if (d.isSubject) {
    return { fill: 'rgba(255, 200, 200, 0.9)', stroke: 'rgba(220, 120, 120, 0.7)' };
  }
  if (d.isObject) {
    return { fill: 'rgba(200, 255, 200, 0.9)', stroke: 'rgba(120, 220, 120, 0.7)' };
  }
  return { fill: 'rgba(240, 240, 240, 0.9)', stroke: 'rgba(200, 200, 200, 0.6)' };
};

// Wrap text by words so each line is <= maxCharsPerLine.
const wrapTextByWords = (text: string, maxCharsPerLine: number): string[] => {
  const words = String(text || '').trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return [];
  const lines: string[] = [];
  let currentLine = '';

  const pushCurrent = () => {
    if (currentLine.length > 0) {
      lines.push(currentLine);
      currentLine = '';
    }
  };

  for (const word of words) {
    if (currentLine.length === 0) {
      currentLine = word;
      continue;
    }
    if (currentLine.length + 1 + word.length <= maxCharsPerLine) {
      currentLine += ' ' + word;
    } else {
      // Start a new line with this whole word (even if it exceeds max)
      pushCurrent();
      currentLine = word;
    }
  }

  pushCurrent();
  return lines;
};

const HyperstructureVisualisation: React.FC<HyperstructureVisualisationProps> = ({ 
  data, 
  isProcessing,
  showStateCausality
}) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const simulationRef = useRef<d3.Simulation<Node, Link> | null>(null);
  const lastDataHashRef = useRef<string>('');
  const zoomBehaviourRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
  const lastTransformRef = useRef<d3.ZoomTransform>(d3.zoomIdentity);
  const hasFittedOnceRef = useRef<boolean>(false);
  // showStateCausality is now controlled by parent via props
  const [popup, setPopup] = useState<{ visible: boolean, nodeId: string | null, stateKey?: string | null, x: number, y: number, value: boolean }>({ visible: false, nodeId: null, stateKey: null, x: 0, y: 0, value: true });
  const [causalitySelection, setCausalitySelection] = useState<{ stateId?: string, stateKey?: string, value: boolean } | null>(null);
  const [stateLabelOverrides, setStateLabelOverrides] = useState<Record<string, string>>({});
  const popupRef = useRef<HTMLDivElement>(null);

  // Process data into nodes and links for visualisation
  const processData = useCallback((data: any) => {
    if (!data || !Array.isArray(data.hyperedges) || data.hyperedges.length === 0) {
      return { nodes: [], links: [] };
    }

    const nodes: Node[] = [];
    const links: Link[] = [];
    const nodeMap = new Map<string, Node>();
    const relationKeyToId = new Map<string, string>();

    const makeRelationKey = (subjects: string[] = [], objects: string[] = [], relationType: string = ''): string => {
      const s = (subjects || []).slice().sort().join('|');
      const o = (objects || []).slice().sort().join('|');
      return `${relationType}__S:${s}__O:${o}`;
    };

    // Process each hyperedge
    data.hyperedges.forEach((hyperedge: any, index: number) => {
      const entities = hyperedge.entities || [];
      const relationType = hyperedge.relation_type || hyperedge.relation_label || `relation_${index}`;
      const subjects = hyperedge.subjects || [];
      const objects = hyperedge.objects || [];
      const contexts = hyperedge.contexts || [];
      const hyperedgeId = hyperedge.id || `he_${index}`;
        
      // Ensure nodes exist for all subjects and objects; accumulate roles across hyperedges
      const ensureEntityNode = (entityId: string) => {
        if (!nodeMap.has(entityId)) {
          const wrapped = wrapTextByWords(entityId, 20);
          const label = (wrapped && wrapped.length > 0) ? wrapped.join('\n') : entityId;
          const node: Node = {
            id: entityId,
            type: 'entity',
            label,
            x: 400 + (Math.random() - 0.5) * 200,
            y: 300 + (Math.random() - 0.5) * 200
          };
          nodes.push(node);
          nodeMap.set(entityId, node);
        }
        return nodeMap.get(entityId)! as Node;
      };

      (subjects || []).forEach((subjectId: string) => {
        if (!subjectId) return;
        const node = ensureEntityNode(subjectId);
        node.isSubject = true;
      });

      (objects || []).forEach((objectId: string) => {
        if (!objectId) return;
        const node = ensureEntityNode(objectId);
        node.isObject = true;
      });

      // Also create any additional entities not covered above (if provided)
      (entities || []).forEach((entity: string) => {
        if (!entity) return;
        ensureEntityNode(entity);
      });
        
      // Create relation node
      const relationId = `relation_${index}`;
      const relationNode: Node = {
        id: relationId,
        type: 'relation',
        label: relationType,
        x: 400 + (Math.random() - 0.5) * 200, // Centre with some random offset
        y: 300 + (Math.random() - 0.5) * 200
      };
      nodes.push(relationNode);
      nodeMap.set(relationId, relationNode);

      // Map content key -> relation node id for later lookup (state links)
      relationKeyToId.set(makeRelationKey(subjects, objects, relationType), relationId);

      // Create links based on subjects and objects
      if (subjects.length > 0 && objects.length > 0) {
        // Connect subjects to relation
        subjects.forEach((subject: string) => {
          links.push({
            source: subject,
            target: relationId,
            directed: true
          });
        });
        
        // Connect relation to objects
        objects.forEach((object: string) => {
          links.push({
            source: relationId,
            target: object,
            directed: true
          });
        });
      } else if (subjects.length > 0) {
        // Only subjects
        subjects.forEach((subject: string) => {
          links.push({
            source: subject,
            target: relationId,
            directed: true
          });
        });
      } else if (objects.length > 0) {
        // Only objects
        objects.forEach((object: string) => {
          links.push({
            source: relationId,
            target: object,
            directed: true
          });
        });
      } else {
        // Fallback: connect entities directly to relation
        if (entities.length >= 2) {
          links.push({
            source: entities[0],
            target: relationId,
            directed: true
          });
          links.push({
            source: relationId,
            target: entities[1],
            directed: true
          });
        } else {
          entities.forEach((entity: string) => {
            links.push({
              source: relationId,
              target: entity,
              directed: true
            });
          });
        }
      }

      // Create context nodes and link from relation to each unique context
      const seenContextIds = new Set<string>();
      contexts.forEach((ctx: any, cIdx: number) => {
        const ctxId = ctx.id || `${hyperedgeId}_ctx_${cIdx}`;
        if (seenContextIds.has(ctxId)) return;
        seenContextIds.add(ctxId);
        const start = ctx.from_time ?? null;
        const end = ctx.to_time ?? null;
        const locRaw = ctx.location_name ?? null;
        const normalized = (v: any) => (v === null || v === undefined || String(v).toLowerCase() === 'unknown' || String(v).trim() === '' ? null : String(v));
        const startText = normalized(start);
        const endText = normalized(end);
        const loc = normalized(locRaw);
        const lines: string[] = [];
        if (startText || endText) {
          lines.push(`${startText ?? 'unknown'}`);
          lines.push(`to ${endText ?? 'unknown'}`);
        }
        if (loc) {
          const locLines = wrapTextByWords(loc, 20);
          if (locLines.length > 0) {
            lines.push(`at ${locLines[0]}`);
            for (let i = 1; i < locLines.length; i++) {
              lines.push(locLines[i]);
            }
          }
        }
        // Skip creating context node entirely if no meaningful temporal or spatial info
        if (lines.length === 0) {
          return;
        }
        const ctxLabel = lines.join('\n');

        if (!nodeMap.has(ctxId)) {
          const node: Node = {
            id: ctxId,
            type: 'context',
            label: ctxLabel,
            x: 400 + (Math.random() - 0.5) * 200,
            y: 300 + (Math.random() - 0.5) * 200
          };
          nodes.push(node);
          nodeMap.set(ctxId, node);
        }
        links.push({ source: relationId, target: ctxId, directed: false });
      });
    });

    // Optionally add state change events and causality links
    if (showStateCausality) {
      const stateEvents: any[] = Array.isArray(data.state_events) ? data.state_events : (Array.isArray(data.state_change_events) ? data.state_change_events : []);
      // Build map from affected_fact relation key -> state node id (for causality linking later)
      const relationKeyToStateId = new Map<string, string>();
      // First pass: ensure state nodes exist and mapping is complete
      stateEvents.forEach((evt: any, idx: number) => {
        if (!evt || evt.fact_type !== 'state_change_event') return;
        const affected = evt.affected_fact || {};
        const affectedKey = makeRelationKey(affected.subjects || [], affected.objects || [], affected.relation_type || '');
        const stateId = evt.id || `state_${affectedKey}`;
        relationKeyToStateId.set(affectedKey, stateId);
        if (!nodeMap.has(stateId)) {
          const node: Node = {
            id: stateId,
            type: 'state_change',
            label: stateLabelOverrides[stateId] || 'State',
            x: 400 + (Math.random() - 0.5) * 200,
            y: 300 + (Math.random() - 0.5) * 200,
            stateAffectedKey: affectedKey
          };
          nodes.push(node);
          nodeMap.set(stateId, node);
        }
      });
      // Second pass: link state->relation and add causality links for selection
      stateEvents.forEach((evt: any) => {
        if (!evt || evt.fact_type !== 'state_change_event') return;
        const affected = evt.affected_fact || {};
        const affectedKey = makeRelationKey(affected.subjects || [], affected.objects || [], affected.relation_type || '');
        const affectedRelationId = relationKeyToId.get(affectedKey);
        const stateId = relationKeyToStateId.get(affectedKey)!;
        if (affectedRelationId) {
          links.push({ source: stateId, target: affectedRelationId, directed: true });
        }
        const selectionMatches = !!causalitySelection && (
          (causalitySelection!.stateId && causalitySelection!.stateId === stateId) ||
          (causalitySelection!.stateKey && causalitySelection!.stateKey === affectedKey)
        );
        if (!selectionMatches) return;
        nodeMap.get(stateId)!.label = causalitySelection!.value ? 'True' : 'False';
        const causedByGroups: any[] = Array.isArray(evt.caused_by) ? evt.caused_by : [];
        causedByGroups.forEach((group: any[]) => {
          if (!Array.isArray(group)) return;
          group.forEach((cause: any) => {
            if (!cause) return;
            const key = makeRelationKey(cause.subjects || [], cause.objects || [], cause.relation_type || '');
            let causeStateId = relationKeyToStateId.get(key);
            if (!causeStateId) {
              causeStateId = `state_${key}`;
              if (!nodeMap.has(causeStateId)) {
                const node: Node = {
                  id: causeStateId,
                  type: 'state_change',
                  label: 'State',
                  x: 400 + (Math.random() - 0.5) * 200,
                  y: 300 + (Math.random() - 0.5) * 200,
                  stateAffectedKey: key
                };
                nodes.push(node);
                nodeMap.set(causeStateId, node);
              }
            }
            const causeVal = Boolean(cause.triggered_by_state);
            nodeMap.get(causeStateId)!.label = causeVal ? 'True' : 'False';
            links.push({ source: causeStateId, target: stateId, directed: true, colour: '#888888', label: 'CAUSES_STATE', labelColour: '#000000' });
          });
        });
        const effects: any[] = Array.isArray(evt.causes) ? evt.causes : [];
        effects.forEach((effect: any) => {
          if (!effect) return;
          const effKey = makeRelationKey(effect.subjects || [], effect.objects || [], effect.relation_type || '');
          let effStateId = relationKeyToStateId.get(effKey);
          if (!effStateId) {
            effStateId = `state_${effKey}`;
            if (!nodeMap.has(effStateId)) {
              const node: Node = {
                id: effStateId,
                type: 'state_change',
                label: 'State',
                x: 400 + (Math.random() - 0.5) * 200,
                y: 300 + (Math.random() - 0.5) * 200,
                stateAffectedKey: effKey
              };
              nodes.push(node);
              nodeMap.set(effStateId, node);
            }
          }
          const effVal = Boolean(effect.triggers_state);
          nodeMap.get(effStateId)!.label = effVal ? 'True' : 'False';
          links.push({ source: stateId, target: effStateId, directed: true, colour: '#888888', label: 'CAUSES_STATE', labelColour: '#000000' });
          const reqs: any[] = Array.isArray(effect.additional_required_states) ? effect.additional_required_states : [];
          reqs.forEach((req: any) => {
            const reqKey = makeRelationKey(req.subjects || [], req.objects || [], req.relation_type || '');
            let reqStateId = relationKeyToStateId.get(reqKey);
            if (!reqStateId) {
              reqStateId = `state_${reqKey}`;
              if (!nodeMap.has(reqStateId)) {
                const node: Node = {
                  id: reqStateId,
                  type: 'state_change',
                  label: 'State',
                  x: 400 + (Math.random() - 0.5) * 200,
                  y: 300 + (Math.random() - 0.5) * 200,
                  stateAffectedKey: reqKey
                };
                nodes.push(node);
                nodeMap.set(reqStateId, node);
              }
            }
            nodeMap.get(reqStateId)!.label = req.state ? 'True' : 'False';
            if (effStateId) {
              const srcId = reqStateId as string;
              const dstId = effStateId as string;
              links.push({ source: srcId, target: dstId, directed: true, colour: '#888888', label: 'REQUIRES_STATE', labelColour: '#000000' });
            }
          });
        });
      });
    }

    const result = { nodes, links };
    
    return result;
  }, [showStateCausality, stateLabelOverrides, causalitySelection]);

  // Main visualisation effect
  useEffect(() => {
    console.log('HyperstructureVisualisation: effect triggered with data:', data, 'isProcessing:', isProcessing);
    
    if (!svgRef.current) return;

    // Check if data has actually changed meaningfully
    const currentDataHash = data ? JSON.stringify({
      hyperedge_count: data.hyperedge_count,
      entities_count: data.entities?.length || 0,
      hyperedges_count: data.hyperedges?.length || 0,
      // Create a hash of hyperedge IDs to detect actual changes
      hyperedges_hash: data.hyperedges ? 
        data.hyperedges.map((h: any) => h.id || `${h.subjects?.join('_')}_${h.relation_type}_${h.objects?.join('_')}`).sort().join('|') : '',
      show_state: showStateCausality,
      state_events_count: Array.isArray((data as any).state_events) ? (data as any).state_events.length : 0,
      selection: causalitySelection ? `${causalitySelection.stateId}:${causalitySelection.value ? 'T' : 'F'}` : 'none',
      overrides: Object.entries(stateLabelOverrides).sort(([a],[b]) => a.localeCompare(b)).map(([k,v]) => `${k}:${v}`).join('|')
    }) : '';
    
    // If data hasn't changed and we already have a visualisation, skip update
    if (currentDataHash === lastDataHashRef.current && simulationRef.current) {
      console.log('HyperstructureVisualisation: data unchanged, skipping visualisation update');
      return;
    }
    
    // Update the last data hash
    lastDataHashRef.current = currentDataHash;

    // Process data into nodes and links for visualisation
    const processedData = processData(data);
    console.log('HyperstructureVisualisation: processed nodes:', processedData.nodes.length, 'links:', processedData.links.length);
    if (showStateCausality) {
      const se = Array.isArray((data as any)?.state_events) ? (data as any).state_events.length : 0;
      console.log(`HyperstructureVisualisation: state toggle ON; state_events in data: ${se}, nodes: ${processedData.nodes.length}, links: ${processedData.links.length}`);
    }

    // If no data, show placeholder
    if (!processedData || processedData.nodes.length === 0) {
      console.log('HyperstructureVisualisation: No data or empty hyperedges, showing placeholder');
      
      const svg = d3.select(svgRef.current);
      const width = 800;
      const height = 600;
      svg.attr("width", width).attr("height", height).attr("viewBox", `0 0 ${width} ${height}`);
      
      // Clear existing content
      svg.selectAll("*").remove();
      
      const placeholderGroup = svg.append("g")
        .attr("class", "placeholder-container")
        .attr("transform", `translate(${width/2}, ${height/2})`);
      
      placeholderGroup.append("text")
        .attr("x", 0)
        .attr("y", -10)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .style("font-size", "20px")
        .style("font-weight", "500")
        .style("fill", "rgba(255, 255, 255, 0.7)")
        .attr("class", "placeholder-text")
        .text("No visualisation created yet");
      
      placeholderGroup.append("text")
        .attr("x", 0)
        .attr("y", 15)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .style("font-size", "16px")
        .style("fill", "rgba(255, 255, 255, 0.5)")
        .attr("class", "placeholder-subtext")
        .text("Load data to create hyperstructure");
      
      return;
    }

    console.log('HyperstructureVisualisation: creating visualisation with data:', data);

    const { nodes, links } = processedData;
    console.log('HyperstructureVisualisation: processed nodes:', nodes.length, 'links:', links.length);

    const svg = d3.select(svgRef.current);
    const parentEl = (svgRef.current?.parentElement as HTMLElement) || null;
    const rect = parentEl ? parentEl.getBoundingClientRect() : null;
    const width = Math.max(300, Math.floor(rect?.width || 800));
    const height = Math.max(300, Math.floor(rect?.height || 600));
    svg.attr("width", width).attr("height", height).attr("viewBox", `0 0 ${width} ${height}`);

    // Clear existing content
    svg.selectAll("*").remove();
      
    // Create arrow markers for directed links (add a grey middle-arrow for causality)
    svg.append("defs").selectAll("marker")
      .data(["arrow", "middle-arrow", "middle-arrow-grey"])
      .enter().append("marker")
      .attr("id", (d: any) => d)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", (d: any) => (d === "middle-arrow" || d === "middle-arrow-grey") ? 5 : 5)
      .attr("refY", 0)
      .attr("markerWidth", 8)
      .attr("markerHeight", 8)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-5L10,0L0,5")
      .attr("fill", (d: any) => d === 'middle-arrow-grey' ? '#888888' : '#000000')
      .attr("stroke", (d: any) => d === 'middle-arrow-grey' ? '#888888' : '#000000')
      .attr("stroke-width", 1);

    // Create a container group for zoom/pan and add link/node groups inside
    const containerGroup = svg.append("g").attr("class", "zoom-container");
    let linkGroup: any = containerGroup.append("g").attr("class", "link-group");
    let nodeGroup: any = containerGroup.append("g").attr("class", "node-group");

    // Initialise or reuse zoom behaviour
    if (!zoomBehaviourRef.current) {
      zoomBehaviourRef.current = d3.zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.2, 5])
        .filter((event: any) => {
          // Allow wheel zoom, mouse drag pan, and touch gestures; block only when altKey is pressed
          if (event.type === 'wheel') return true;
          if (event.type === 'mousedown' || event.type === 'touchstart' || event.type === 'pointerdown') return true;
          return !event.altKey;
        })
        .on("zoom", (event: any) => {
          const t = event.transform as d3.ZoomTransform;
          lastTransformRef.current = t;
          containerGroup.attr("transform", t.toString());
        });
      svg.call(zoomBehaviourRef.current as any).on("dblclick.zoom", null);
    } else {
      svg.call(zoomBehaviourRef.current as any).on("dblclick.zoom", null);
      // Re-apply last known transform on re-render
      containerGroup.attr("transform", lastTransformRef.current.toString());
    }

    // Initialise or update simulation
    if (!simulationRef.current) {
      console.log('HyperstructureVisualisation: Creating new simulation');
      simulationRef.current = d3.forceSimulation(nodes)
        .force("link", d3.forceLink(links).id((d: any) => d.id).distance(120))
        .force("charge", d3.forceManyBody().strength(-120))
        .force("center", d3.forceCenter(width / 2, height / 2))
        .force("collision", d3.forceCollide().radius(50))
        .alpha(1) // Start with full alpha
        .alphaDecay(0.02) // Slower decay for smoother animation
        .velocityDecay(0.4); // Less velocity decay for better movement
    } else {
      // Only update simulation if nodes or links have actually changed
      const currentNodes = simulationRef.current.nodes();
      const currentLinks = (simulationRef.current.force("link") as d3.ForceLink<Node, Link>).links();
      
      const nodesChanged = nodes.length !== currentNodes.length || 
        nodes.some((node, i) => node.id !== currentNodes[i]?.id);
      const linksChanged = links.length !== currentLinks.length ||
        links.some((link, i) => `${link.source}-${link.target}` !== `${currentLinks[i]?.source}-${currentLinks[i]?.target}`);
      
      if (nodesChanged || linksChanged) {
        console.log('HyperstructureVisualisation: updating existing simulation (data changed)');
        simulationRef.current.nodes(nodes);
        (simulationRef.current.force("link") as d3.ForceLink<Node, Link>).links(links);
        simulationRef.current.alpha(1).restart(); // Start with full alpha
      } else {
        console.log('HyperstructureVisualisation: simulation data unchanged, skipping update');
      }
    }

    // Force initial ticks to stabilise positions before fitting
    if (simulationRef.current) {
      console.log('HyperstructureVisualisation: forcing initial ticks');
      for (let i = 0; i < 30; i++) {
        simulationRef.current.tick();
      }
      // Force a redraw of the nodes
      nodeGroup.selectAll("g.node")
        .attr("transform", (d: any) => `translate(${d.x || width/2},${d.y || height/2})`);
    }

    // Auto-fit to content with padding
    const fitToContent = () => {
      if (!zoomBehaviourRef.current) return;
      if (!nodes || nodes.length === 0) return;
      const padding = 60; // visual padding around content
      let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
      nodes.forEach((n: any) => {
        const r = getNodeRadius(n.label);
        minX = Math.min(minX, (n.x || 0) - r);
        maxX = Math.max(maxX, (n.x || 0) + r);
        minY = Math.min(minY, (n.y || 0) - r);
        maxY = Math.max(maxY, (n.y || 0) + r);
      });
      const contentWidth = Math.max(1, maxX - minX);
      const contentHeight = Math.max(1, maxY - minY);
      const scale = Math.min(
        (width - padding) / contentWidth,
        (height - padding) / contentHeight
      ) * 0.95; // slightly smaller to give breathing room
      const clampedScale = Math.max(0.2, Math.min(5, scale));
      const translateX = width / 2 - clampedScale * (minX + contentWidth / 2);
      const translateY = height / 2 - clampedScale * (minY + contentHeight / 2);
      const target = d3.zoomIdentity.translate(translateX, translateY).scale(clampedScale);
      lastTransformRef.current = target;
      const selection = svg as any;
      if (!hasFittedOnceRef.current) {
        // First fit: apply immediately to avoid visible flicker
        selection.call((zoomBehaviourRef.current as any).transform, target);
        hasFittedOnceRef.current = true;
      } else {
        selection.transition().duration(300).call((zoomBehaviourRef.current as any).transform, target);
      }
    };

    // Auto-fit when data changes
    fitToContent();

    // Update links (key includes label to distinguish causality vs relation links)
    const linkSelection = linkGroup.selectAll("g.link-group")
      .data(links, (d: any) => {
        const src = typeof d.source === 'object' ? (d.source as any).id : d.source;
        const tgt = typeof d.target === 'object' ? (d.target as any).id : d.target;
        const lbl = d.label || '';
        return `${src}->${tgt}::${lbl}`;
      });

    // Remove old links
    linkSelection.exit().remove();

    // Enter new links
    const linkEnter = linkSelection.enter()
      .append("g")
      .attr("class", "link-group");

    // Add the line to new links
    linkEnter.append("path")
      .attr("class", "hyperedge-link")
      .attr("stroke", (d: any) => d.colour || d.color || "#4a4a4a")
      .attr("stroke-width", 2)
      .attr("fill", "none")
      .attr("stroke-dasharray", (d: any) => d.dashed ? "6,3" : null);

    // Add the middle arrow for directed links
    linkEnter.append("path")
      .attr("class", "middle-arrow")
      .attr("stroke", "none")
      .attr("fill", "none")
      .attr("marker-end", (d: any) => {
        if (!d.directed) return null;
        return d.label ? "url(#middle-arrow-grey)" : "url(#middle-arrow)";
      });

    // Add labels for styled/causality links (append after shapes so they render above)
    linkEnter.append("text")
      .attr("class", "link-label")
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .style("font-size", "10px")
      .style("font-weight", "600")
      .style("pointer-events", "none")
      .style("fill", (d: any) => d.labelColour || d.labelColor || d.colour || d.color || "#333");

    // Ensure marker-end is correct for both new and existing links
    linkGroup.selectAll("g.link-group")
      .select("path.middle-arrow")
      .attr("marker-end", (d: any) => {
        if (!d.directed) return null;
        return d.label ? "url(#middle-arrow-grey)" : "url(#middle-arrow)";
      });

    // Update styles for existing links
    linkGroup.selectAll("g.link-group")
      .select("path.hyperedge-link")
      .attr("stroke", (d: any) => d.colour || d.color || "#4a4a4a")
      .attr("stroke-dasharray", (d: any) => d.dashed ? "6,3" : null);

    // Update nodes
    const nodeSelection = nodeGroup.selectAll("g.node")
      .data(nodes, (d: any) => d.id);

    // Remove old nodes
    nodeSelection.exit().remove();

    // Enter new nodes
    const nodeEnter = nodeSelection.enter()
      .append("g")
      .attr("class", (d: any) => {
        const base = `node ${d.type}`;
        if (d.type === 'entity') {
          const roleClasses = [d.isSubject ? 'subject' : null, d.isObject ? 'object' : null].filter(Boolean).join(' ');
          return roleClasses ? `${base} ${roleClasses}` : base;
        }
        return base;
      })
      .attr("transform", (d: any) => `translate(${d.x || width/2},${d.y || height/2})`) // Set initial position
      .call(d3.drag<any, Node>()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended));

    // Add shapes to new nodes: circle for entity/context, diamond for relation
    nodeEnter.each(function(this: SVGGElement, d: any) {
      const group = d3.select(this);
      const radius = getNodeRadius(d.label);
      const colors = getNodeColours(d);
      const fillColor = colors.fill;
      const strokeColor = colors.stroke;
      if (d.type === 'relation') {
        group.append('path')
          .attr('d', `M0,-${radius} L${radius},0 L0,${radius} L-${radius},0 Z`)
          .attr('fill', fillColor)
          .attr('stroke', strokeColor)
          .attr('stroke-width', 2)
          .style('filter', 'drop-shadow(0 4px 8px rgba(0, 0, 0, 0.3))');
      } else {
        group.append('circle')
          .attr('r', radius)
          .attr('fill', fillColor)
          .attr('stroke', strokeColor)
          .attr('stroke-width', 2)
          .style('filter', 'drop-shadow(0 4px 8px rgba(0, 0, 0, 0.3))');
      }
      // Click-to-open popup for state nodes
      if (d.type === 'state_change') {
        group.on('click', (event: any, nd: any) => {
          event.stopPropagation();
          const svgEl = svgRef.current as SVGSVGElement | null;
          if (!svgEl) return;
          const rect = svgEl.getBoundingClientRect();
          const relX = event.clientX - rect.left;
          const relY = event.clientY - rect.top;
          setPopup({ visible: true, nodeId: nd.id, stateKey: nd.stateAffectedKey || null, x: relX, y: relY, value: true });
        });
      }
    });

    // Update colors for existing nodes on every render
    nodeGroup.selectAll('g.node').each(function(this: SVGGElement, d: any) {
      const colors = getNodeColours(d);
      const group = d3.select(this);
      const shape = d.type === 'relation' ? group.select('path') : group.select('circle');
      shape.attr('fill', colors.fill).attr('stroke', colors.stroke);
    });

    // Update labels for all nodes (reflect any label changes like True/False)
    nodeGroup.selectAll('g.node').select('text.node-label').each(function(this: SVGTextElement, d: any) {
      const textSel = d3.select(this);
      const labelText = String(d.label || 'Unknown');
      const lines = labelText.split('\n');
      const lineHeightEm = 1.1;
      // Clear and rebuild tspans
      textSel.text(null);
      const startDy = -((lines.length - 1) / 2) * lineHeightEm;
      lines.forEach((line, i) => {
        textSel.append('tspan')
          .attr('x', 0)
          .attr('dy', (i === 0 ? startDy : lineHeightEm) + 'em')
          .text(line);
      });
    });

    // Add labels to new nodes (support multi-line with tspans)
    nodeEnter.append("text")
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .style("font-size", (d: any) => {
        // Base font size on the longest line rather than total length
        const lines = String(d.label || 'Unknown').split('\n');
        const longest = lines.reduce((m, l) => Math.max(m, l.length), 0);
        return Math.max(10, Math.min(14, 16 - longest * 0.3)) + "px";
      })
      .style("font-weight", "500")
      .style("fill", (d: any) => {
        return d.type === 'entity' ? "#000000" : "#333333";
      })
      .attr("class", "node-label")
      .each(function(this: SVGTextElement, d: any) {
        const textSel = d3.select(this);
        const labelText = String(d.label || 'Unknown');
        const lines = labelText.split('\n');
        const lineHeightEm = 1.1; // line-height in ems
        // Clear any default text content
        textSel.text(null);
        // Centre block vertically: start the first line above centre
        const startDy = -((lines.length - 1) / 2) * lineHeightEm;
        lines.forEach((line, i) => {
          textSel.append('tspan')
            .attr('x', 0)
            .attr('dy', (i === 0 ? startDy : lineHeightEm) + 'em')
            .text(line);
        });
      });

    // Update positions on simulation tick
    simulationRef.current.on("tick", () => {
      // Compute current viewport bounds in simulation coordinates so the "force wall" matches the visible box
      const t = lastTransformRef.current;
      const simLeft = (0 - t.x) / t.k;
      const simRight = (width - t.x) / t.k;
      const simTop = (0 - t.y) / t.k;
      const simBottom = (height - t.y) / t.k;
      // Apply boundary constraints and repellent forces to keep nodes within the visualisation area
      nodes.forEach((node: any) => {
        // Get the node radius for boundary calculations
        const radius = getNodeRadius(node.label);
        const padding = 10; // Add some padding to the boundaries
        const repellentZone = 150; // Distance from edge where repellent force starts (increased)
        
        // Apply repellent forces from boundaries
        let repellentForceX = 0;
        let repellentForceY = 0;
        const repellentStrength = 3.0; // Increased strength of the repellent force
        
        // Left boundary repellent force (simulation-space bounds)
        if (node.x - radius < simLeft + repellentZone) {
          const distance = Math.max(0, (simLeft + repellentZone) - (node.x - radius));
          const force = Math.pow(distance / repellentZone, 2) * repellentStrength; // Quadratic falloff
          repellentForceX += force;
        }
        
        // Right boundary repellent force
        if (node.x + radius > simRight - repellentZone) {
          const distance = Math.max(0, (node.x + radius) - (simRight - repellentZone));
          const force = Math.pow(distance / repellentZone, 2) * repellentStrength; // Quadratic falloff
          repellentForceX -= force;
        }
        
        // Top boundary repellent force
        if (node.y - radius < simTop + repellentZone) {
          const distance = Math.max(0, (simTop + repellentZone) - (node.y - radius));
          const force = Math.pow(distance / repellentZone, 2) * repellentStrength; // Quadratic falloff
          repellentForceY += force;
        }
        
        // Bottom boundary repellent force
        if (node.y + radius > simBottom - repellentZone) {
          const distance = Math.max(0, (node.y + radius) - (simBottom - repellentZone));
          const force = Math.pow(distance / repellentZone, 2) * repellentStrength; // Quadratic falloff
          repellentForceY -= force;
        }
        
        // Apply repellent forces to velocity (with some damping)
        if (node.vx !== undefined) node.vx += repellentForceX * 0.15;
        if (node.vy !== undefined) node.vy += repellentForceY * 0.15;
        
        // Constrain x position (left and right boundaries) - hard boundary
        if (node.x - radius < simLeft + padding) {
          node.x = simLeft + radius + padding;
          node.vx = 0; // Stop horizontal velocity when hitting boundary
        } else if (node.x + radius > simRight - padding) {
          node.x = simRight - radius - padding;
          node.vx = 0; // Stop horizontal velocity when hitting boundary
        }
        
        // Constrain y position (top and bottom boundaries) - hard boundary
        if (node.y - radius < simTop + padding) {
          node.y = simTop + radius + padding;
          node.vy = 0; // Stop vertical velocity when hitting boundary
        } else if (node.y + radius > simBottom - padding) {
          node.y = simBottom - radius - padding;
          node.vy = 0; // Stop vertical velocity when hitting boundary
        }
      });

      // Update link positions
      linkGroup.selectAll("g.link-group").each(function(this: any, d: any) {
        const group = d3.select(this);
        const sNode = d.source as any;
        const tNode = d.target as any;
        // Compute boundary-to-boundary segment
        const startPt = computeBoundaryPoint(sNode, tNode.x, tNode.y);
        const endPt = computeBoundaryPoint(tNode, sNode.x, sNode.y);
        // Update the visible line
        group.select("path.hyperedge-link")
          .attr("d", `M${startPt.x},${startPt.y}L${endPt.x},${endPt.y}`);
        
        // Update the middle arrow (positioned at midpoint of visible segment)
        if (d.directed) {
          const midX = (startPt.x + endPt.x) / 2;
          const midY = (startPt.y + endPt.y) / 2;
          const dx = endPt.x - startPt.x;
          const dy = endPt.y - startPt.y;
          const angle = Math.atan2(dy, dx) * 180 / Math.PI;
          
          group.select("path.middle-arrow")
            .attr("d", `M${midX - 5},${midY}L${midX + 5},${midY}`)
            .attr("transform", `rotate(${angle} ${midX} ${midY})`);
        }

        // Update link label position/content if present
        if (d.label) {
          const midX = (startPt.x + endPt.x) / 2;
          const midY = (startPt.y + endPt.y) / 2;
          // Offset label slightly above the line centre along the normal to avoid covering the arrow
          const dx = endPt.x - startPt.x;
          const dy = endPt.y - startPt.y;
          const len = Math.max(1, Math.hypot(dx, dy));
          const nx = -dy / len; // normal x (perpendicular to the line)
          const ny = dx / len;  // normal y
          const offset = 8; // pixels offset above the line
          group.select("text.link-label")
            .attr("x", midX + nx * offset)
            .attr("y", midY + ny * offset)
            .text(d.label)
            .style("fill", (d2: any) => d2.labelColour || d2.labelColor || d2.colour || d2.color || "#333");
        } else {
          group.select("text.link-label").text("");
        }
      });

      // Update node positions
      nodeGroup.selectAll("g.node")
        .attr("transform", (d: any) => `translate(${d.x},${d.y})`);

      // Keep container transform in sync (ensures zoom/pan applied to both links and nodes)
      containerGroup.attr("transform", lastTransformRef.current.toString());
    });

    // Drag functions
    function dragstarted(event: any, d: Node) {
      if (event.sourceEvent && typeof event.sourceEvent.stopPropagation === 'function') {
        event.sourceEvent.stopPropagation();
      }
      if (!event.active && simulationRef.current) simulationRef.current.alphaTarget(0.3).restart();
      d.fx = d.x;
      d.fy = d.y;
    }

    function dragged(event: any, d: Node) {
      // Apply boundary constraints during drag
      const radius = getNodeRadius(d.label as any);
      const padding = 10; // Add some padding to the boundaries
      // Current viewport bounds in simulation coords
      const t = lastTransformRef.current;
      const simLeft = (0 - t.x) / t.k;
      const simRight = (width - t.x) / t.k;
      const simTop = (0 - t.y) / t.k;
      const simBottom = (height - t.y) / t.k;
      
      // Constrain x position
      let newX = event.x;
      if (newX - radius < simLeft + padding) newX = simLeft + radius + padding;
      if (newX + radius > simRight - padding) newX = simRight - radius - padding;
      
      // Constrain y position
      let newY = event.y;
      if (newY - radius < simTop + padding) newY = simTop + radius + padding;
      if (newY + radius > simBottom - padding) newY = simBottom - radius - padding;
      
      d.fx = newX;
      d.fy = newY;
    }

    function dragended(event: any, d: Node) {
      if (!event.active && simulationRef.current) simulationRef.current.alphaTarget(0);
      d.fx = null;
      d.fy = null;
    }

  }, [data, isProcessing, showStateCausality, processData, causalitySelection, stateLabelOverrides]);

  // Cleanup effect to stop simulation when component unmounts or data changes significantly
  useEffect(() => {
    return () => {
      if (simulationRef.current) {
        simulationRef.current.stop();
        simulationRef.current = null;
      }
    };
  }, []);

  // Dismiss popup when clicking outside
  useEffect(() => {
    const onDocMouseDown = (e: MouseEvent) => {
      if (!popup.visible) return;
      const el = popupRef.current;
      const target = e.target as unknown as HTMLElement | null;
      if (el && target && el.contains(target)) return;
      setPopup(p => ({ ...p, visible: false }));
    };
    document.addEventListener('mousedown', onDocMouseDown);
    return () => document.removeEventListener('mousedown', onDocMouseDown);
  }, [popup.visible]);

  // Effect to handle significant data changes (like when processing starts/stops)
  useEffect(() => {
    // If processing state changes from true to false, it means we're done
    // and should ensure the visualisation is stable
    if (!isProcessing && simulationRef.current) {
      // Let the simulation settle
      setTimeout(() => {
        if (simulationRef.current) {
          simulationRef.current.alpha(0.1).alphaDecay(0.01);
        }
      }, 1000);
    }
  }, [isProcessing]);

  return (
    <div className="visualisation-container">
      <div className="svg-container" style={{ position: 'relative' }}>
        <svg ref={svgRef}></svg>
        {showStateCausality && popup.visible && popup.nodeId && (
          <div
            ref={popupRef}
            style={{ position: 'absolute', left: popup.x, top: popup.y, background: 'white', border: '1px solid #ccc', borderRadius: 6, padding: '8px 10px', boxShadow: '0 2px 8px rgba(0,0,0,0.15)', transform: 'translate(-50%, -100%)', zIndex: 10 }}
          >
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              <button onClick={() => setPopup(p => ({ ...p, value: !p.value }))}>
                {popup.value ? 'True' : 'False'}
              </button>
              <button onClick={() => {
                if (!popup.nodeId) return;
                setCausalitySelection({ stateId: popup.nodeId, stateKey: popup.stateKey || undefined, value: popup.value });
                setStateLabelOverrides(prev => ({ ...prev, [popup.nodeId as string]: popup.value ? 'True' : 'False' }));
                setPopup(p => ({ ...p, visible: false }));
              }}>
                Show causality
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default HyperstructureVisualisation; 