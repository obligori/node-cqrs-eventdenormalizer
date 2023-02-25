var expect = require('expect.js'),
  EventDispatcher = require('../.denormalizer/lib/eventDispatcher');

describe('eventDispatcher', function () {

  describe('creating a new instance', function () {

    describe('without tree argument', function () {

      it('it should throw an error', function () {

        expect(function () {
          new EventDispatcher();
        }).to.throwError(/tree/);

      });

    });

    describe('without definition argument', function () {

      it('it should throw an error', function () {

        expect(function () {
          new EventDispatcher({ getViewBuilders: function () {} });
        }).to.throwError(/definition/);

      });

    });

    describe('with all correct arguments', function () {

      it('it should not throw an error', function () {

        expect(function () {
          new EventDispatcher({ getViewBuilders: function () {} }, {});
        }).not.to.throwError();

      });

      describe('calling getTargetInformation', function () {

        describe('without event argument', function () {

          it('it should throw an error', function () {

            var evtDisp = new EventDispatcher({ getViewBuilders: function () {} }, {});
            expect(function () {
              evtDisp.getTargetInformation();
            }).to.throwError(/event/);

          });

        });

        describe('with event argument', function () {

          it('it should not throw an error', function () {

            var evtDisp = new EventDispatcher({ getViewBuilders: function () {} }, {});
            expect(function () {
              evtDisp.getTargetInformation({});
            }).not.to.throwError();

          });

          describe('passing a definition with all infos', function () {

            it('it should return the correct target infos', function () {

              var evtDisp = new EventDispatcher({ getViewBuilders: function () {} }, { name: 'evtName', version: 'v', aggregate: 'agg', context: 'ctx' });
              var target = evtDisp.getTargetInformation({ evtName: 'evtNameSpec', v: 3, agg: 'aggName', ctx: 'myCtx' });
              expect(target.name).to.eql('evtNameSpec');
              expect(target.version).to.eql(3);
              expect(target.aggregate).to.eql('aggName');
              expect(target.context).to.eql('myCtx');

            });

          });

          describe('passing a definition with less infos', function () {

            it('it should return the correct target infos', function () {

              var evtDisp = new EventDispatcher({ getViewBuilders: function () {} }, { name: 'evtName' });
              var target = evtDisp.getTargetInformation({ evtName: 'evtNameSpec' });
              expect(target.name).to.eql('evtNameSpec');
              expect(target.version).to.eql(0);

            });

          });

        });

      });

      describe('calling dispatch', function () {

        describe('with no matching saga', function () {

          it('it should callback without an error', function (done) {

            var evtDisp = new EventDispatcher({ getViewBuilders: function () {
              return [];
            }}, { name: 'evtName' });
            evtDisp.dispatch({ evtName: 'evtNameSpec' }, function (err, sagaModels) {
              expect(err).not.to.be.ok();
              expect(sagaModels).to.be.an('array');
              expect(sagaModels.length).to.eql(0);
              done();
            });

          });

        });

        describe('with matching saga', function () {

          it('it should call his handle function', function (done) {

            var calledBack1 = false;
            var calledBack2 = false;
            var evtDisp = new EventDispatcher({ getViewBuilders: function () {
              return [{ denormalize: function (evt, clb) {
                expect(evt.evtName).to.eql('evtNameSpec');
                expect(clb).to.be.a('function');
                calledBack1 = true;
                clb(null);
              }},
                { denormalize: function (evt, clb) {
                  expect(evt.evtName).to.eql('evtNameSpec');
                  expect(clb).to.be.a('function');
                  calledBack2 = true;
                  clb(null);
                }}];
            }}, { name: 'evtName' });

            evtDisp.dispatch({ evtName: 'evtNameSpec' }, function (err) {
              expect(err).not.to.be.ok();
              expect(calledBack1).to.eql(true);
              expect(calledBack2).to.eql(true);
              done();
            });

          });

        });

      });

    });

  });

});
