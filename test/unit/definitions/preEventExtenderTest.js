var expect = require('expect.js'),
  _ = require('lodash'),
  DefinitionBase = require('../../.denormalizer/lib/definitionBase'),
  PreEventExtender = require('../../.denormalizer/lib/definitions/preEventExtender'),
  api = require('../../../');

describe('preEventExtender definition', function () {

  describe('creating a new preEventExtender definition', function () {

    describe('without any arguments', function () {

      it('it should throw an error', function () {

        expect(function () {
          api.definePreEventExtender();
        }).to.throwError(/function/);

      });

    });

    describe('without preEventExtender function', function () {

      it('it should throw an error', function () {

        expect(function () {
          api.definePreEventExtender(null);
        }).to.throwError(/function/);

      });

    });

    describe('with a wrong preEventExtender function', function () {

      it('it should throw an error', function () {

        expect(function () {
          api.definePreEventExtender(null, 'not a function');
        }).to.throwError(/function/);

      });

    });

    describe('with a correct preEventExtender function', function () {

      it('it should not throw an error', function () {

        expect(function () {
          api.definePreEventExtender(null, function () {
          });
        }).not.to.throwError();

      });

      it('it should return a correct object', function () {

        var evtExtFn = function () {
        };
        var evtExt = api.definePreEventExtender(null, evtExtFn);
        expect(evtExt).to.be.a(DefinitionBase);
        expect(evtExt).to.be.a(PreEventExtender);
        expect(evtExt.evtExtFn).to.eql(evtExtFn);
        expect(evtExt.definitions).to.be.an('object');
        expect(evtExt.definitions.notification).to.be.an('object');
        expect(evtExt.definitions.event).to.be.an('object');
        expect(evtExt.defineNotification).to.be.a('function');
        expect(evtExt.defineEvent).to.be.a('function');
        expect(evtExt.defineOptions).to.be.a('function');

        expect(evtExt.extend).to.be.a('function');
        expect(evtExt.useCollection).to.be.a('function');

      });

    });

    describe('with some meta infos and a correct preEventExtender function', function () {

      it('it should not throw an error', function () {

        expect(function () {
          api.definePreEventExtender({ name: 'eventName', version: 3 }, function () {
          });
        }).not.to.throwError();

      });

      it('it should return a correct object', function () {

        var evtExtFn = function () {
        };
        var evtExt = api.definePreEventExtender({ name: 'eventName', version: 3 }, evtExtFn);
        expect(evtExt).to.be.a(DefinitionBase);
        expect(evtExt).to.be.a(PreEventExtender);
        expect(evtExt.evtExtFn).to.eql(evtExtFn);
        expect(evtExt.definitions).to.be.an('object');
        expect(evtExt.definitions.notification).to.be.an('object');
        expect(evtExt.definitions.event).to.be.an('object');
        expect(evtExt.defineNotification).to.be.a('function');
        expect(evtExt.defineEvent).to.be.a('function');
        expect(evtExt.defineOptions).to.be.a('function');

        expect(evtExt.extend).to.be.a('function');
        expect(evtExt.useCollection).to.be.a('function');

      });

    });

    describe('extending an event', function () {

      var evtExt;

      describe('having an event extender function that wants expects 3 arguments', function () {

        describe('not defining an id', function () {

          it('it should work as expected', function (done) {
            var extendedEvt = { ext: 'evt' };
            var evtExtFn = function (evt, col, callback) {
              expect(evt.my).to.eql('evt');
              expect(col.name).to.eql('myCol');
              callback(null, extendedEvt);
            };
            evtExt = api.definePreEventExtender({
              name: 'eventName',
              version: 3
            }, evtExtFn);

            evtExt.useCollection({
              name: 'myCol'
            });

            evtExt.extend({ my: 'evt' }, function (err, eEvt) {
              expect(err).not.to.be.ok();
              expect(eEvt).to.eql(extendedEvt);
              done();
            });
          });

        });

        describe('defining an id', function () {

          it('it should work as expected', function (done) {
            var viewM = { my: 'view' };
            var extendedEvt = { ext: 'evt', myId: '1234' };
            var evtExtFn = function (evt, vm, callback) {
              expect(evt.my).to.eql('evt');
              expect(vm.id).to.eql('1234');
              callback(null, extendedEvt);
            };
            evtExt = api.definePreEventExtender({
              name: 'eventName',
              version: 3,
              id: 'myId'
            }, evtExtFn);

            evtExt.useCollection({
              name: 'myCol',
              getNewId: function (callback) { callback(null, 'newId'); },
              loadViewModel: function (id, callback) { viewM.id = id; callback(null, viewM); }
            });

            evtExt.extend({ my: 'evt', myId: '1234' }, function (err, eEvt) {
              expect(err).not.to.be.ok();
              expect(eEvt).to.eql(extendedEvt);
              done();
            });
          });

        });

      });

      describe('having an event extender function that wants expects 1 argument', function () {

        it('it should work as expected', function (done) {
          var extendedEvt = { ext: 'evt' };
          var evtExtFn = function (evt) {
            expect(evt.my).to.eql('evt');
            return extendedEvt;
          };
          evtExt = api.definePreEventExtender({
            name: 'eventName',
            version: 3
          }, evtExtFn);

          evtExt.useCollection({
            name: 'myCol'
          });

          evtExt.extend({ my: 'evt' }, function (err, eEvt) {
            expect(err).not.to.be.ok();
            expect(eEvt).to.eql(extendedEvt);
            done();
          });
        });

      });

      describe('having an event extender function that wants expects 2 argument', function () {

        describe('not defining an id', function () {

          it('it should work as expected', function (done) {
            var extendedEvt = { ext: 'evt' };
            var evtExtFn = function (evt, callback) {
              expect(evt.my).to.eql('evt');
              callback(null, extendedEvt);
            };
            evtExt = api.definePreEventExtender({
              name: 'eventName',
              version: 3
            }, evtExtFn);

            evtExt.useCollection({
              name: 'myCol'
            });

            evtExt.extend({ my: 'evt' }, function (err, eEvt) {
              expect(err).not.to.be.ok();
              expect(eEvt).to.eql(extendedEvt);
              done();
            });
          });

        });

        describe('defining an id', function () {

          describe('but not passing it in the event', function () {

            it('it should work as expected', function (done) {
              var extendedEvt = { ext: 'evt' };
              var viewM = { my: 'view' };
              var evtExtFn = function (evt, vm) {
                expect(evt.my).to.eql('evt');
                expect(vm).to.eql(viewM);
                expect(vm.id).to.eql('newId');
                return extendedEvt;
              };
              evtExt = api.definePreEventExtender({
                name: 'eventName',
                version: 3,
                id: 'id'
              }, evtExtFn);

              evtExt.useCollection({
                name: 'myCol',
                getNewId: function (callback) { callback(null, 'newId'); },
                loadViewModel: function (id, callback) { viewM.id = id; callback(null, viewM); }
              });

              evtExt.extend({ my: 'evt' }, function (err, eEvt) {
                expect(err).not.to.be.ok();
                expect(eEvt).to.eql(extendedEvt);
                done();
              });
            });

          });

          describe('and passing it in the event', function () {

            it('it should work as expected', function (done) {
              var extendedEvt = { ext: 'evt' };
              var viewM = { my: 'view' };
              var evtExtFn = function (evt, vm) {
                expect(evt.my).to.eql('evt');
                expect(vm).to.eql(viewM);
                expect(vm.id).to.eql('idInEvt');
                return extendedEvt;
              };
              evtExt = api.definePreEventExtender({
                name: 'eventName',
                version: 3,
                id: 'id'
              }, evtExtFn);

              evtExt.useCollection({
                name: 'myCol',
                getNewId: function (callback) { callback(null, 'newId'); },
                loadViewModel: function (id, callback) { viewM.id = id; callback(null, viewM); }
              });

              evtExt.extend({ my: 'evt', id: 'idInEvt' }, function (err, eEvt) {
                expect(err).not.to.be.ok();
                expect(eEvt).to.eql(extendedEvt);
                done();
              });
            });

          });

        });

      });

    });

  });

});
