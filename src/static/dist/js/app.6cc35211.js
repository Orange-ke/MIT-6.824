(function(t){function e(e){for(var r,i,o=e[0],c=e[1],u=e[2],f=0,d=[];f<o.length;f++)i=o[f],Object.prototype.hasOwnProperty.call(a,i)&&a[i]&&d.push(a[i][0]),a[i]=0;for(r in c)Object.prototype.hasOwnProperty.call(c,r)&&(t[r]=c[r]);l&&l(e);while(d.length)d.shift()();return s.push.apply(s,u||[]),n()}function n(){for(var t,e=0;e<s.length;e++){for(var n=s[e],r=!0,o=1;o<n.length;o++){var c=n[o];0!==a[c]&&(r=!1)}r&&(s.splice(e--,1),t=i(i.s=n[0]))}return t}var r={},a={app:0},s=[];function i(e){if(r[e])return r[e].exports;var n=r[e]={i:e,l:!1,exports:{}};return t[e].call(n.exports,n,n.exports,i),n.l=!0,n.exports}i.m=t,i.c=r,i.d=function(t,e,n){i.o(t,e)||Object.defineProperty(t,e,{enumerable:!0,get:n})},i.r=function(t){"undefined"!==typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(t,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(t,"__esModule",{value:!0})},i.t=function(t,e){if(1&e&&(t=i(t)),8&e)return t;if(4&e&&"object"===typeof t&&t&&t.__esModule)return t;var n=Object.create(null);if(i.r(n),Object.defineProperty(n,"default",{enumerable:!0,value:t}),2&e&&"string"!=typeof t)for(var r in t)i.d(n,r,function(e){return t[e]}.bind(null,r));return n},i.n=function(t){var e=t&&t.__esModule?function(){return t["default"]}:function(){return t};return i.d(e,"a",e),e},i.o=function(t,e){return Object.prototype.hasOwnProperty.call(t,e)},i.p="/";var o=window["webpackJsonp"]=window["webpackJsonp"]||[],c=o.push.bind(o);o.push=e,o=o.slice();for(var u=0;u<o.length;u++)e(o[u]);var l=c;s.push([0,"chunk-vendors"]),n()})({0:function(t,e,n){t.exports=n("56d7")},"034f":function(t,e,n){"use strict";n("85ec")},"43ef":function(t,e,n){"use strict";n("b712")},"56d7":function(t,e,n){"use strict";n.r(e);n("e260"),n("e6cf"),n("cca6"),n("a79d");var r=n("2b0e"),a=function(){var t=this,e=t.$createElement,r=t._self._c||e;return r("div",{attrs:{id:"app"}},[r("div",[r("img",{attrs:{alt:"Vue logo",src:n("8ca0")}}),r("Header",{attrs:{msg:"Welcome to Raft view"}})],1),r("div",[r("RaftView")],1)])},s=[],i=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticClass:"raft_container"},[n("h2",[t._v(t._s(t.phrase))]),n("div",[n("button",{staticClass:"button",attrs:{disabled:t.disabled},on:{click:t.startNotify}},[t._v(t._s(t.start?t.end?"end":"running":"start"))])]),n("div",{staticClass:"raft_body"},[n("ul",t._l(t.rafts,(function(e,r){return n("li",{key:r,staticClass:"raft",attrs:{id:"raft"+r}},[n("span",{class:"raft_index state"+e.state+" connected"+e.connected+" crash"+e.crash+" "},[t._v(t._s(e.id))]),t._l(e.logs,(function(e,r){return n("span",{key:r,staticClass:"log"},[n("div",{staticClass:"log_item"},[n("span",[t._v(t._s(e.log_term))])]),n("div",{staticClass:"log_item"},[n("span",[t._v(t._s(e.log_index))])])])}))],2)})),0)])])},o=[],c=n("1da1"),u=(n("d3b7"),n("96cf"),{name:"RaftView",data:function(){return{start:!1,disabled:!1,end:!1,interval:{},phrase:"wait to start",rafts:[]}},created:function(){clearInterval(this.interval)},methods:{getRaftList:function(){var t=this;return Object(c["a"])(regeneratorRuntime.mark((function e(){var n,r;return regeneratorRuntime.wrap((function(e){while(1)switch(e.prev=e.next){case 0:return e.next=2,t.$http.get("raft_view");case 2:n=e.sent,r=n.data,t.rafts=r.nodes,t.phrase=r.phrase,r.end&&(t.end=r.end,clearInterval(t.interval));case 7:case"end":return e.stop()}}),e)})))()},startNotify:function(){var t=this;return Object(c["a"])(regeneratorRuntime.mark((function e(){var n,r;return regeneratorRuntime.wrap((function(e){while(1)switch(e.prev=e.next){case 0:return t.disabled=!0,e.next=3,t.$http.get("raft_view",{params:{start:!0}}).finally((function(){t.disabled=!0}));case 3:n=e.sent,r=n.data,t.start=!0,t.rafts=r.nodes,t.phrase=r.phrase,t.interval=setInterval(t.getRaftList,100);case 9:case"end":return e.stop()}}),e)})))()}},destroyed:function(){clearInterval(this.interval)}}),l=u,f=(n("916d"),n("2877")),d=Object(f["a"])(l,i,o,!1,null,"4161397a",null),p=d.exports,v=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticClass:"header"},[n("h1",[t._v(t._s(t.msg))])])},h=[],b={name:"Header",props:{msg:String}},g=b,_=(n("43ef"),Object(f["a"])(g,v,h,!1,null,"5bfb364e",null)),m=_.exports,y={name:"App",components:{RaftView:p,Header:m}},w=y,x=(n("034f"),Object(f["a"])(w,a,s,!1,null,null,null)),O=x.exports,j=n("bc3a"),R=n.n(j);R.a.defaults.baseURL="http://192.168.43.99:9000/",r["a"].prototype.$http=R.a,r["a"].config.productionTip=!1,r["a"].config.productionTip=!1,new r["a"]({render:function(t){return t(O)}}).$mount("#app")},"85ec":function(t,e,n){},"8ca0":function(t,e,n){t.exports=n.p+"img/raft.ba4052b9.jpg"},"916d":function(t,e,n){"use strict";n("fdc3")},b712:function(t,e,n){},fdc3:function(t,e,n){}});
//# sourceMappingURL=app.6cc35211.js.map