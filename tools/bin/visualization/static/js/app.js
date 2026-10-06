function dragstarted(d) {
  if (!d3.event.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x;
  d.fy = d.y;
}

function dragged(d) {
  d.fx = d3.event.x;
  d.fy = d3.event.y;
}

function dragended(d) {
  if (!d3.event.active) simulation.alphaTarget(0);
  d.fx = null;
  d.fy = null;
}

var origins = new Set([
  'ffed335bc92f6f27d33a8b5a12866328b1e70117',
  '598bd849259c97118c11759b9895fda171610a51',
  'cd178baaa144a3c86c7ee9080936c73da8b4b597',
])

const bitfieldWidth = 200;

const radius = 8;

function bitfieldString(bitfield) {
  var rows = [];
  var cursor = 0;
  for (var i = 0; i < bitfield.length; i++) {
    if (i > 0 && i % bitfieldWidth == 0) {
      cursor++;
    }
    if (rows.length == cursor) {
      rows.push([]);
    }
    rows[cursor].push(bitfield[i]);
  }
  return 'Pieces: ' + rows.map(row => row.map(b => b ? '1' : '0').join('')).join('\n');
}

function connKey(sourceID, targetID) {
  if (sourceID < targetID) {
    return sourceID + ':' + targetID;
  }
  return targetID + ':' + sourceID;
}

class Graph {
  constructor(torrent, startTime) {
    this.torrent = torrent;
    this.startTime = startTime;
    this.curTime = startTime;

    this.w = window.innerWidth;
    this.h = window.innerHeight - 120;

    this.header = d3.select('#graph').append('div').attr('class', 'header');
    this.headerTime = this.header.append('p').append('pre');
    this.headerElapsed = this.header.append('p').append('pre').text('Elapsed: 0s');
    this.headerNumPeers = this.header.append('p').append('pre');
    this.headerTorrent = this.header.append('p').append('pre').text('Torrent: ' + torrent);
    this.headerPeerID = this.header.append('p').append('pre');
    this.headerBitfield = this.header.append('p').append('pre');

    this.svg = d3.select('#graph').append('svg')
      .attr('width', this.w)
      .attr('height', this.h);

    this.svg.append('g').attr('class', 'links');

    this.svg.append('g').attr('class', 'blinks');

    this.svg.append('g').attr('class', 'nodes');

    this.peers = [];
    this.conns = [];
    this.blacklist = [];

    this.peerIndexByID = {};

    this.connKeys = new Set();

    this.simulation = d3.forceSimulation(this.peers)
      .force('charge', d3.forceManyBody().strength(-70).distanceMax(400))
      .force('center', d3.forceCenter(this.w/2, this.h/2))
      .force('link', d3.forceLink(this.conns).id(d => d.id));

    this.update();

    this.currHighlightedPeer = null;

    this.simulation.on('tick', this.tick.bind(this));
  }

  checkPeer(id) {
    if (!(id in this.peerIndexByID)) {
      throw {
        message: 'not found',
        peer: id,
      }
    }
  }

  getPeer(id) {
    this.checkPeer(id);
    return this.peers[this.peerIndexByID[id]];
  }

  update() {
    this.simulation.nodes(this.peers);
    this.simulation.force('link').links(this.conns);

    // Draw blacklisted connection links.

    // Remove expired blacklist items. Loop backwards so splice still works.
    for (var i = this.blacklist.length - 1; i >= 0; i--) {
      if (this.blacklist[i].expiresAt < this.curTime) {
        this.blacklist.splice(i, 1);
      }
    }

    this.blink = this.svg.select('.blinks').selectAll('.blink').data(this.blacklist);

    this.blink
      .enter()
      .append('line')
      .attr('class', 'blink')
      .attr('stroke-width', 1)
      .attr('stroke', '#ef9d88');

    this.blink.exit().remove();

    // Draw connection links.

    this.link = this.svg.select('.links').selectAll('.link').data(this.conns);

    this.link
      .enter()
      .append('line')
      .attr('class', 'link')
      .attr('stroke-width', 1)
      .attr('stroke', '#999999');

    this.link.exit().remove();

    // Draw peer nodes.

    this.node = this.svg.select('.nodes').selectAll('.node').data(this.peers);

    var drag = d3.drag()
      .on('start', d => {
        if (!d3.event.active) this.simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
      })
      .on('drag', d => {
        d.fx = d3.event.x;
        d.fy = d3.event.y;
      })
      .on('end', d => {
        if (!d3.event.active) this.simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
      })

    this.node
      .enter()
      .append('circle')
      .attr('class', 'node')
      .attr('r', radius)
      .attr('stroke-width', 1.5)
      .call(drag)
      .on('click', d => {
        this.headerPeerID.text('PeerID: ' + d.id);
        this.headerBitfield.text(bitfieldString(d.bitfield));
        if (this.currHighlightedPeer) {
          this.currHighlightedPeer.highlight = false;
        }
        this.currHighlightedPeer = d;
        d.highlight = true;
        this.node.attr('id', d => d.highlight ? 'highlight' : null);
        this.blacklist = d.blacklist;
        this.update();
      });

    this.node
      .attr('fill', d => {
        if (origins.has(d.id)) {
          return 'hsl(230, 100%, 50%)';
        }
        if (d.complete) {
          return 'hsl(120, 100%, 50%)';
        }
        var completed = 0;
        d.bitfield.forEach(b => completed += b ? 1 : 0);
        var percentComplete = 100.0 * completed / d.bitfield.length;
        return 'hsl(55, ' + Math.ceil(percentComplete) + '%, 50%)';
      })
      .each(d => {
        if (d.highlight) {
          this.headerBitfield.text(bitfieldString(d.bitfield));
        }
      });

    this.node.exit().remove();

    this.simulation.alphaTarget(0.05).restart();
  }

  addPeer(id, bitfield) {
    if (id in this.peerIndexByID) {
      throw {
        message: 'duplicate peer',
        peer: id,
      }
    }
    this.peerIndexByID[id] = this.peers.length;
    this.peers.push({
      type: 'peer',
      id: id,
      x: this.w / 2,
      y: this.h / 2,
      complete: false,
      bitfield: bitfield,
      highlight: false,
      blacklist: [],
    });
    this.headerNumPeers.text('Num peers: ' + this.peers.length);
  }

  addActiveConn(sourceID, targetID) {
    this.checkPeer(sourceID);
    this.checkPeer(targetID);
    var k = connKey(sourceID, targetID);
    if (this.connKeys.has(k)) {
      return;
    }
    this.conns.push({
      type: 'conn',
      source: sourceID,
      target: targetID,
    });
    this.connKeys.add(k)
  }

  removeActiveConn(sourceID, targetID) {
    var k = connKey(sourceID, targetID);
    if (!this.connKeys.has(k)) {
      return;
    }
    var removed = false;
    for (var i = 0; i < this.conns.length; i++) {
      var curK = connKey(this.conns[i].source.id, this.conns[i].target.id);
      if (curK == k) {
        this.conns.splice(i, 1);
        removed = true;
      }
    }
  }

  blacklistConn(sourceID, targetID, duration) {
    var source = this.getPeer(sourceID);
    var target = this.getPeer(targetID);
    source.blacklist.push({
      source: source,
      target: target,
      expiresAt: this.curTime + duration,
    })
  }

  receivePiece(id, piece) {
    this.getPeer(id).bitfield[piece] = true;
  }

  completePeer(id) {
    var p = this.getPeer(id);
    p.complete = true;
    for (var i = 0; i < p.bitfield.length; i++) {
      p.bitfield[i] = true;
    }
  }

  tick() {
    this.node
      .attr('cx', d => {
        d.x = Math.max(radius, Math.min(this.w - radius, d.x));
        return d.x;
      })
      .attr('cy', d => {
        d.y = Math.max(radius, Math.min(this.h - radius, d.y));
        return d.y;
      });

    this.link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);

    this.blink
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);
  }

  setTime(t) {
    var d = new Date(t);
    this.headerTime.text(d.toString());
    var elapsed = (t - this.startTime) / 1000;
    this.headerElapsed.text('Elapsed: ' + elapsed + 's');
    this.curTime = t;
  }
}

d3.request('http://' + location.host + '/events').get(req => {
  var events = JSON.parse(req.response);
  var graph = new Graph(events[0].torrent, Date.parse(events[0].ts));

  // Maps peer id to list of events which occurred before the peer was added
  // to the graph. Early events are possible in cases where a connection is
  // added before the torrent is opened, which is valid.
  var earlyEvents = {};

  function applyEvent(event) {
    try {
      switch (event.event) {
        case 'add_torrent':
          graph.addPeer(event.self, event.bitfield);
          if (event.self in earlyEvents) {
            earlyEvents[event.self].forEach(e => applyEvent(e))
          }
          break;
        case 'add_active_conn':
          graph.addActiveConn(event.self, event.peer);
          break;
        case 'drop_active_conn':
          graph.removeActiveConn(event.self, event.peer);
          break;
        case 'receive_piece':
          graph.receivePiece(event.self, event.piece);
          break;
        case 'torrent_complete':
          graph.completePeer(event.self);
          break;
        case 'blacklist_conn':
          graph.blacklistConn(event.self, event.peer, parseInt(event.duration_ms));
          break;
      }
    } catch (err) {
      if (err.message == 'not found') {
        if (!(err.peer in earlyEvents)) {
          earlyEvents[err.peer] = [];
        }
        earlyEvents[err.peer].push(event);
      } else {
        console.log('unhandled error: ' + err);
      }
    }
  }

  // Every interval seconds, we read all events that occur within that interval
  // and apply them to the graph. This gives the illusion of events occuring in
  // real-time.
  const interval = 100;

  function readEvents(i, until) {
    if (i >= events.length) {
      return;
    }
    graph.setTime(until);
    while (i < events.length && Date.parse(events[i].ts) < until) {
      applyEvent(events[i]);
      i++;
    }
    graph.update();
    setTimeout(() => readEvents(i, until + interval), interval);
  }

  readEvents(0, Date.parse(events[0].ts) + interval);
});
